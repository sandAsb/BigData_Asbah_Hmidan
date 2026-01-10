package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.utils.MemoryUtils;

import java.time.Duration;
import java.util.*;

/**
 * DataProvider is a MEMORY AUTHORITY + COLUMN OWNER.
 * - Stores column partitions (chunks)
 * - Enforces budget, can reject stores (visible to miner)
 * - Emits AttributeFullyLoaded when completed AND fully stored
 * - Can initiate column stealing when overloaded
 */
public class DataProvider extends AbstractBehavior<DataProvider.Message> {

    public static final ServiceKey<Message> SERVICE_KEY =
            ServiceKey.create(Message.class, "dataProviderService");

    public interface Message extends AkkaSerializable {}

    //////////////////////////
    // Miner adapter wiring //
    //////////////////////////

    public static final class SetMinerAdapters implements Message {
        public final ActorRef<DependencyMiner.Message> miner;
        public final ActorRef<AttributeFullyLoaded> fullyLoadedAdapter;
        public final ActorRef<StoreRejected> storeRejectedAdapter;
        public final ActorRef<ProviderAvailable> providerAvailableAdapter;
        public final ActorRef<StealColumnRequest> stealRequestAdapter;

        public SetMinerAdapters(
                ActorRef<DependencyMiner.Message> miner,
                ActorRef<AttributeFullyLoaded> fullyLoadedAdapter,
                ActorRef<StoreRejected> storeRejectedAdapter,
                ActorRef<ProviderAvailable> providerAvailableAdapter,
                ActorRef<StealColumnRequest> stealRequestAdapter
        ) {
            this.miner = miner;
            this.fullyLoadedAdapter = fullyLoadedAdapter;
            this.storeRejectedAdapter = storeRejectedAdapter;
            this.providerAvailableAdapter = providerAvailableAdapter;
            this.stealRequestAdapter = stealRequestAdapter;
        }
    }

    /////////////////////
    // Store & control //
    /////////////////////

    /** Store a CHUNK of a column for an attribute (miner reads batch-wise). */
    public static final class StorePartition implements Message {
        public final int attribute;
        public final String[] columnChunk;
        public StorePartition(int attribute, String[] columnChunk) {
            this.attribute = attribute;
            this.columnChunk = columnChunk;
        }
    }

    /** Miner tells provider that an attribute has received all chunks. */
    public static final class MarkAttributeComplete implements Message {
        public final int attribute;
        public MarkAttributeComplete(int attribute) {
            this.attribute = attribute;
        }
    }

    /** Worker requests the FULL column for an attribute. */
    public static final class RequestPartition implements Message {
        public final int attribute;
        public final ActorRef<ResponsePartition> replyTo;
        public RequestPartition(int attribute, ActorRef<ResponsePartition> replyTo) {
            this.attribute = attribute;
            this.replyTo = replyTo;
        }
    }

    public static final class ResponsePartition implements Message {
        public final int attribute;
        public final String[] column;
        public ResponsePartition(int attribute, String[] column) {
            this.attribute = attribute;
            this.column = column;
        }
    }

    /** Optional: worker frees provider memory for an attribute after validation. */
    public static final class ReleaseAttribute implements Message {
        public final int attribute;
        public ReleaseAttribute(int attribute) {
            this.attribute = attribute;
        }
    }

    ///////////////////////////
    // Provider -> Miner msgs //
    ///////////////////////////

    public static final class AttributeFullyLoaded implements AkkaSerializable {
        public final int attribute;
        public final ActorRef<DataProvider.Message> provider;
        public AttributeFullyLoaded(int attribute, ActorRef<DataProvider.Message> provider) {
            this.attribute = attribute;
            this.provider = provider;
        }
    }

    public static final class StoreRejected implements AkkaSerializable {
        public final int attribute;
        public final ActorRef<DataProvider.Message> provider;
        public final long usedBytes;
        public final long budgetBytes;
        public final String reason;

        public StoreRejected(int attribute, ActorRef<DataProvider.Message> provider, long usedBytes, long budgetBytes, String reason) {
            this.attribute = attribute;
            this.provider = provider;
            this.usedBytes = usedBytes;
            this.budgetBytes = budgetBytes;
            this.reason = reason;
        }
    }

    public static final class ProviderAvailable implements AkkaSerializable {
        public final ActorRef<DataProvider.Message> provider;
        public final long usedBytes;
        public final long budgetBytes;
        public ProviderAvailable(ActorRef<DataProvider.Message> provider, long usedBytes, long budgetBytes) {
            this.provider = provider;
            this.usedBytes = usedBytes;
            this.budgetBytes = budgetBytes;
        }
    }

    public static final class StealColumnRequest implements AkkaSerializable {
        public final int attribute;
        public final ActorRef<DataProvider.Message> fromProvider;
        public final long approxBytes;

        public StealColumnRequest(int attribute, ActorRef<DataProvider.Message> fromProvider, long approxBytes) {
            this.attribute = attribute;
            this.fromProvider = fromProvider;
            this.approxBytes = approxBytes;
        }
    }

    ///////////////////////
    // Stealing protocol  //
    ///////////////////////

    /** Miner -> Provider: transfer column to target provider. */
    public static final class InitiateSteal implements Message {
        public final int attribute;
        public final ActorRef<DataProvider.Message> target;
        public InitiateSteal(int attribute, ActorRef<DataProvider.Message> target) {
            this.attribute = attribute;
            this.target = target;
        }
    }

    /** Provider -> Provider: column payload for steal. */
    public static final class StealColumnData implements Message {
        public final int attribute;
        public final List<String[]> chunks;
        public final boolean completed;
        public StealColumnData(int attribute, List<String[]> chunks, boolean completed) {
            this.attribute = attribute;
            this.chunks = chunks;
            this.completed = completed;
        }
    }

    private enum RetryTick implements Message { INSTANCE }

    public static Behavior<Message> create() {
        return Behaviors.setup(ctx -> Behaviors.withTimers(timers -> new DataProvider(ctx, timers)));
    }

    private final TimerScheduler<Message> timers;

    // attribute -> stored chunks
    private final Map<Integer, List<String[]>> chunks = new HashMap<>();
    // attribute -> total rows across chunks
    private final Map<Integer, Integer> sizeRows = new HashMap<>();
    // attributes that are complete
    private final Set<Integer> completed = new HashSet<>();
    // attribute -> waiting replyTo's
    private final Map<Integer, List<ActorRef<ResponsePartition>>> waiters = new HashMap<>();
    // queued stores if memory exceeded
    private final ArrayDeque<StorePartition> pendingStores = new ArrayDeque<>();

    private final long budgetBytes;
    private long usedBytes = 0L;

    // miner adapters
    private ActorRef<AttributeFullyLoaded> fullyLoadedOut;
    private ActorRef<StoreRejected> storeRejectedOut;
    private ActorRef<ProviderAvailable> providerAvailableOut;
    private ActorRef<StealColumnRequest> stealRequestOut;

    private boolean lastReportedOverloaded = false;

    private DataProvider(ActorContext<Message> ctx, TimerScheduler<Message> timers) {
        super(ctx);
        this.timers = timers;

        long working = Math.max(0L, MemoryUtils.bytesMax() - MemoryUtils.bytesUsed());
        this.budgetBytes = (long) (0.70 * working);

        // Register so DependencyMiner can discover providers
        ctx.getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, ctx.getSelf()));

        // Periodic retry tick for queued stores
        timers.startTimerAtFixedRate(RetryTick.INSTANCE, RetryTick.INSTANCE, Duration.ofMillis(250));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(SetMinerAdapters.class, this::onSetMinerAdapters)
                .onMessage(StorePartition.class, this::onStore)
                .onMessage(MarkAttributeComplete.class, this::onComplete)
                .onMessage(RequestPartition.class, this::onRequest)
                .onMessage(ReleaseAttribute.class, this::onRelease)
                .onMessage(InitiateSteal.class, this::onInitiateSteal)
                .onMessage(StealColumnData.class, this::onStealColumnData)
                .onMessageEquals(RetryTick.INSTANCE, this::onRetry)
                .build();
    }

    private Behavior<Message> onSetMinerAdapters(SetMinerAdapters msg) {
        this.fullyLoadedOut = msg.fullyLoadedAdapter;
        this.storeRejectedOut = msg.storeRejectedAdapter;
        this.providerAvailableOut = msg.providerAvailableAdapter;
        this.stealRequestOut = msg.stealRequestAdapter;
        return this;
    }

    private Behavior<Message> onStore(StorePartition msg) {
        boolean stored = tryStoreNow(msg.attribute, msg.columnChunk);
        if (!stored) {
            // No dropping — queue for retry AND tell miner (global backpressure)
            pendingStores.addLast(msg);
            if (storeRejectedOut != null) {
                storeRejectedOut.tell(new StoreRejected(
                        msg.attribute,
                        getContext().getSelf(),
                        usedBytes,
                        budgetBytes,
                        "Budget exceeded or chunk split queued"
                ));
            }

            maybeRequestSteal();
        } else {
            tryReplyWaiters(msg.attribute);
            maybeReportAvailable();
            maybeRequestSteal();
            maybeEmitFullyLoadedIfReady(msg.attribute);
        }
        return this;
    }

    private Behavior<Message> onComplete(MarkAttributeComplete msg) {
        completed.add(msg.attribute);
        tryReplyWaiters(msg.attribute);
        maybeEmitFullyLoadedIfReady(msg.attribute);
        return this;
    }

    private Behavior<Message> onRequest(RequestPartition msg) {
        if (isReady(msg.attribute)) {
            msg.replyTo.tell(new ResponsePartition(msg.attribute, materialize(msg.attribute)));
        } else {
            waiters.computeIfAbsent(msg.attribute, k -> new ArrayList<>()).add(msg.replyTo);
        }
        return this;
    }

    private Behavior<Message> onRelease(ReleaseAttribute msg) {
        List<String[]> list = chunks.remove(msg.attribute);
        sizeRows.remove(msg.attribute);
        completed.remove(msg.attribute);

        if (list != null) {
            for (String[] c : list) usedBytes -= safeSize(c);
        }

        drainPendingStores();
        maybeReportAvailable();
        return this;
    }

    private Behavior<Message> onRetry() {
        drainPendingStores();
        maybeReportAvailable();
        maybeRequestSteal();
        // After draining, some attributes might become fully loaded
        // (safe to check all completed attributes)
        for (int a : completed) maybeEmitFullyLoadedIfReady(a);
        return this;
    }

    private Behavior<Message> onInitiateSteal(InitiateSteal msg) {
        List<String[]> list = chunks.get(msg.attribute);
        if (list == null || list.isEmpty()) return this;

        boolean comp = completed.contains(msg.attribute);

        // send copy of chunks
        msg.target.tell(new StealColumnData(msg.attribute, new ArrayList<>(list), comp));

        // free local
        chunks.remove(msg.attribute);
        sizeRows.remove(msg.attribute);
        completed.remove(msg.attribute);

        for (String[] c : list) usedBytes -= safeSize(c);

        drainPendingStores();
        maybeReportAvailable();
        return this;
    }

    private Behavior<Message> onStealColumnData(StealColumnData msg) {
        // attempt to store all chunks (best-effort)
        for (String[] c : msg.chunks) {
            // store chunk; if can't, queue (will apply backpressure here too)
            if (!tryStoreNow(msg.attribute, c)) {
                pendingStores.addLast(new StorePartition(msg.attribute, c));
                if (storeRejectedOut != null) {
                    storeRejectedOut.tell(new StoreRejected(
                            msg.attribute,
                            getContext().getSelf(),
                            usedBytes,
                            budgetBytes,
                            "Steal chunk queued due to budget"
                    ));
                }
                break;
            }
        }

        if (msg.completed) completed.add(msg.attribute);

        // if now ready, emit
        maybeEmitFullyLoadedIfReady(msg.attribute);
        maybeReportAvailable();
        return this;
    }

    private boolean isReady(int attribute) {
        return completed.contains(attribute) && chunks.containsKey(attribute) && !chunks.get(attribute).isEmpty();
    }

    private void maybeEmitFullyLoadedIfReady(int attribute) {
        if (fullyLoadedOut == null) return;
        if (isReady(attribute) && pendingStores.isEmpty()) {
            fullyLoadedOut.tell(new AttributeFullyLoaded(attribute, getContext().getSelf()));
        }
    }

    private void tryReplyWaiters(int attribute) {
        List<ActorRef<ResponsePartition>> w = waiters.get(attribute);
        if (w == null || w.isEmpty()) return;
        if (!isReady(attribute)) return;

        String[] full = materialize(attribute);
        for (ActorRef<ResponsePartition> r : w) r.tell(new ResponsePartition(attribute, full));
        waiters.remove(attribute);
    }

    private boolean tryStoreNow(int attribute, String[] chunk) {
        long bytes = safeSize(chunk);

        // Split huge chunk if needed (repartition)
        if (bytes > budgetBytes && chunk.length > 1) {
            int mid = chunk.length / 2;
            String[] left = Arrays.copyOfRange(chunk, 0, mid);
            String[] right = Arrays.copyOfRange(chunk, mid, chunk.length);
            pendingStores.addFirst(new StorePartition(attribute, right));
            pendingStores.addFirst(new StorePartition(attribute, left));
            return false;
        }

        if (usedBytes + bytes > budgetBytes) return false;

        chunks.computeIfAbsent(attribute, k -> new ArrayList<>()).add(chunk);
        sizeRows.merge(attribute, chunk.length, Integer::sum);
        usedBytes += bytes;
        return true;
    }

    private void drainPendingStores() {
        if (pendingStores.isEmpty()) return;

        int bound = pendingStores.size();
        while (bound-- > 0 && !pendingStores.isEmpty()) {
            StorePartition p = pendingStores.peekFirst();
            if (tryStoreNow(p.attribute, p.columnChunk)) {
                pendingStores.removeFirst();
                tryReplyWaiters(p.attribute);
                maybeEmitFullyLoadedIfReady(p.attribute);
            } else {
                break;
            }
        }
    }

    private String[] materialize(int attribute) {
        List<String[]> list = chunks.get(attribute);
        if (list == null) return new String[0];
        int total = sizeRows.getOrDefault(attribute, 0);

        String[] out = new String[total];
        int pos = 0;
        for (String[] c : list) {
            System.arraycopy(c, 0, out, pos, c.length);
            pos += c.length;
        }
        return out;
    }

    private long safeSize(String[] arr) {
        try {
            return MemoryUtils.byteSizeOf(arr);
        } catch (Throwable t) {
            return 64L + (long) arr.length * 8L;
        }
    }

    private void maybeReportAvailable() {
        if (providerAvailableOut == null) return;

        // simple policy: report available once we drop below 90% budget or if we were overloaded before
        boolean overloadedNow = usedBytes > (long) (0.90 * budgetBytes) || !pendingStores.isEmpty();
        if (lastReportedOverloaded && !overloadedNow) {
            providerAvailableOut.tell(new ProviderAvailable(getContext().getSelf(), usedBytes, budgetBytes));
        }
        lastReportedOverloaded = overloadedNow;
    }

    private void maybeRequestSteal() {
        if (stealRequestOut == null) return;

        // If we're beyond 95% budget and we have at least one substantial column, request stealing
        if (usedBytes <= (long) (0.95 * budgetBytes)) return;

        // pick largest attribute (approx by total rows chunks)
        int bestAttr = -1;
        int bestRows = -1;
        for (Map.Entry<Integer, Integer> e : sizeRows.entrySet()) {
            int a = e.getKey();
            int rows = e.getValue();
            if (rows > bestRows) { bestRows = rows; bestAttr = a; }
        }
        if (bestAttr < 0) return;

        long approx = 0L;
        List<String[]> list = chunks.get(bestAttr);
        if (list != null) for (String[] c : list) approx += safeSize(c);

        stealRequestOut.tell(new StealColumnRequest(bestAttr, getContext().getSelf(), approx));
    }
}