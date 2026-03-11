package de.ddm.actors.profiling;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.Master;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import it.unimi.dsi.fastutil.booleans.*;
import it.unimi.dsi.fastutil.ints.*;
import lombok.*;

import java.io.File;
import java.util.*;

/**
 * Pure coordinator:
 * - Builds attribute mappings
 * - Maintains authoritative ownership map: attribute -> owning DataProvider
 * - Coordinates ingestion pause/resume based on provider memory responses
 * - Waits for AttributeFullyLoaded for ALL attributes, then creates global validation task queue
 * - Workers pull work; results are collected and forwarded to ResultCollector
 */
public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    ////////////////////
    // Messages
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {}

    @NoArgsConstructor public static class StartMessage implements Message {}
    @NoArgsConstructor public static class ShutdownMessage implements Message {}

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class HeaderMessage implements Message {
        int readerId;
        String[] header;
    }

    /** batch is ROWS: [numRows][numCols] */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class BatchMessage implements Message {
        int readerId;
        String[][] batch;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        ActorRef<DependencyWorker.Message> dependencyWorker;
        ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP; // can be null
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class DataProviderListingMessage implements Message {
        Receptionist.Listing listing;
    }

    /** Provider -> Miner: an attribute is fully loaded and ready (all chunks stored + marked complete). */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class AttributeFullyLoadedMessage implements Message {
        int attribute;
        ActorRef<DataProvider.Message> provider; // who claims readiness
    }

    /** Provider -> Miner: provider rejected store (memory pressure). */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class StoreRejectedMessage implements Message {
        int attribute;
        ActorRef<DataProvider.Message> provider;
        long usedBytes;
        long budgetBytes;
        String reason;
    }

    /** Provider -> Miner: provider became available again (after draining). */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class ProviderAvailableMessage implements Message {
        ActorRef<DataProvider.Message> provider;
        long usedBytes;
        long budgetBytes;
    }

    /** Provider -> Miner: ask to steal a column to relieve memory pressure. */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class StealColumnRequestMessage implements Message {
        int attribute;
        ActorRef<DataProvider.Message> fromProvider;
        long approxBytes;
    }

    /** Worker -> Miner: ask for work. */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class RequestWorkMessage implements Message {
        ActorRef<DependencyWorker.Message> worker;
    }

    /** Miner -> Worker: validate lhs ⊆ rhs. */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class ValidatePairMessage implements Message {
        int lhs;
        int rhs;
    }

    /** Worker -> Miner: result of validation. */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class ValidationResultMessage implements Message {
        int lhs;
        int rhs;
        boolean isIND;
    }

    /** Worker -> Miner: request authoritative owner for attribute. */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class GetOwnerMessage implements Message {
        int attribute;
        ActorRef<DependencyWorker.Message> replyTo;
    }

    /** Miner -> Worker: owner reply. */
    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class OwnerReplyMessage implements Message {
        int attribute;
        ActorRef<DataProvider.Message> owner;
    }

    /** Miner internal: assign initial ownership after providers known. */
    private static final class AssignOwnershipTick implements Message {
        private static final long serialVersionUID = 1L;
    }

    ////////////////////
    // Actor Creation
    ////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";

    public static final ServiceKey<Message> dependencyMinerService =
            ServiceKey.create(Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create(ActorRef<Master.Message> master) {
        return Behaviors.setup(ctx -> new DependencyMiner(ctx, master));
    }

    ////////////////////
    // State
    ////////////////////

    private final ActorRef<Master.Message> master;

    private final File[] inputFiles;
    private final String[][] headerLines;
    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final BooleanList inputReaderFinished;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers = new ArrayList<>();
    private final ActorRef<ResultCollector.Message> resultCollector;

    // attribute mappings
    private HashMap<File, int[]> file2attribute;
    private File[] attribute2file;
    private String[] attribute2name;
    private int numAttributes = 0;

    // providers discovered
    private final List<ActorRef<DataProvider.Message>> dataProviders = new ArrayList<>();

    // authoritative ownership
    private final Int2ObjectMap<ActorRef<DataProvider.Message>> ownerOf = new Int2ObjectOpenHashMap<>();

    // ingestion buffering + pause
    private static final class PendingStore {
        final int attribute;
        final String[] columnChunk;
        PendingStore(int attribute, String[] columnChunk) {
            this.attribute = attribute;
            this.columnChunk = columnChunk;
        }
    }
    private final ArrayDeque<PendingStore> pendingStores = new ArrayDeque<>();
    private boolean headersReady = false;
    private boolean readingStarted = false;
    private boolean ownershipAssigned = false;
    private boolean ingestionPaused = false;

    private static final int batchSize = 10_000;

    // completion tracking
    private final BooleanList attributeReady = new BooleanArrayList(); // AttributeFullyLoaded received
    private int readyCount = 0;
    private boolean validationPhaseStarted = false;

    // global task queue (ordered pairs lhs!=rhs)
    private final ArrayDeque<long[]> taskQueue = new ArrayDeque<>();
    private long totalTasks = 0;
    private long completedTasks = 0;

    // adapters for provider -> miner messages
    private final ActorRef<DataProvider.AttributeFullyLoaded> fullyLoadedAdapter;
    private final ActorRef<DataProvider.StoreRejected> storeRejectedAdapter;
    private final ActorRef<DataProvider.ProviderAvailable> providerAvailableAdapter;
    private final ActorRef<DataProvider.StealColumnRequest> stealRequestAdapter;

    ////////////////////
    // Constructor
    ////////////////////

    private DependencyMiner(ActorContext<Message> context, ActorRef<Master.Message> master) {
        super(context);
        this.master = master;

        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[inputFiles.length][];
        this.inputReaders = new ArrayList<>(inputFiles.length);
        this.inputReaderFinished = new BooleanArrayList();

        for (int i = 0; i < inputFiles.length; i++) {
            inputReaders.add(context.spawn(
                    InputReader.create(i, inputFiles[i]),
                    InputReader.DEFAULT_NAME + "_" + i
            ));
            inputReaderFinished.add(false);
        }

        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);

        // expose miner + subscribe to providers
        context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));

        final ActorRef<Receptionist.Listing> dpListingAdapter =
                context.messageAdapter(Receptionist.Listing.class, DataProviderListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DataProvider.SERVICE_KEY, dpListingAdapter));

        // provider -> miner adapters
        this.fullyLoadedAdapter =
                context.messageAdapter(DataProvider.AttributeFullyLoaded.class,
                        m -> new AttributeFullyLoadedMessage(m.attribute, m.provider));

        this.storeRejectedAdapter =
                context.messageAdapter(DataProvider.StoreRejected.class,
                        m -> new StoreRejectedMessage(m.attribute, m.provider, m.usedBytes, m.budgetBytes, m.reason));

        this.providerAvailableAdapter =
                context.messageAdapter(DataProvider.ProviderAvailable.class,
                        m -> new ProviderAvailableMessage(m.provider, m.usedBytes, m.budgetBytes));

        this.stealRequestAdapter =
                context.messageAdapter(DataProvider.StealColumnRequest.class,
                        m -> new StealColumnRequestMessage(m.attribute, m.fromProvider, m.approxBytes));
    }

    ////////////////////
    // Behavior
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::onStart)
                .onMessage(HeaderMessage.class, this::onHeader)
                .onMessage(BatchMessage.class, this::onBatch)
                .onMessage(RegistrationMessage.class, this::onRegistration)
                .onMessage(DataProviderListingMessage.class, this::onProviderListing)

                .onMessage(AttributeFullyLoadedMessage.class, this::onAttributeFullyLoaded)
                .onMessage(StoreRejectedMessage.class, this::onStoreRejected)
                .onMessage(ProviderAvailableMessage.class, this::onProviderAvailable)
                .onMessage(StealColumnRequestMessage.class, this::onStealRequest)

                .onMessage(RequestWorkMessage.class, this::onRequestWork)
                .onMessage(ValidationResultMessage.class, this::onValidationResult)
                .onMessage(GetOwnerMessage.class, this::onGetOwner)

                .onMessage(ShutdownMessage.class, this::onShutdown)
                .onMessage(AssignOwnershipTick.class, msg -> { tryAssignOwnership(); return this; })
                .build();
    }

    private Behavior<Message> onStart(StartMessage msg) {
        for (ActorRef<InputReader.Message> r : inputReaders)
            r.tell(new InputReader.ReadHeaderMessage(getContext().getSelf()));
        return this;
    }

    private Behavior<Message> onHeader(HeaderMessage msg) {
        headerLines[msg.readerId] = msg.header;
        if (Arrays.stream(headerLines).anyMatch(Objects::isNull)) return this;

        buildAttributeMappings();
        headersReady = true;

        // init attributeReady size
        attributeReady.clear();
        for (int i = 0; i < numAttributes; i++) attributeReady.add(false);
        readyCount = 0;

        tryStartReading();
        return this;
    }

    private Behavior<Message> onRegistration(RegistrationMessage msg) {
        if (!dependencyWorkers.contains(msg.dependencyWorker)) {
            dependencyWorkers.add(msg.dependencyWorker);
        }
        tryStartReading();
        return this;
    }

    private Behavior<Message> onProviderListing(DataProviderListingMessage msg) {
        Set<ActorRef<DataProvider.Message>> providers =
                msg.getListing().getServiceInstances(DataProvider.SERVICE_KEY);

        dataProviders.clear();
        dataProviders.addAll(providers);

        // providers changed -> try (re)assign ownership ONLY if not assigned yet
        tryAssignOwnership();

        // flush buffered stores once we have providers + ownership
        flushPendingStores();

        tryStartReading();
        return this;
    }

    private void tryAssignOwnership() {
        if (ownershipAssigned) return;
        if (!headersReady) return;
        if (dataProviders.isEmpty()) return;

        // initial authoritative ownership: stable assignment done ONCE
        ownerOf.clear();
        for (int a = 0; a < numAttributes; a++) {
            ActorRef<DataProvider.Message> p = dataProviders.get(a % dataProviders.size());
            ownerOf.put(a, p);

            // let provider know miner adapters (so provider can signal back)
            p.tell(new DataProvider.SetMinerAdapters(
                    getContext().getSelf(),
                    fullyLoadedAdapter,
                    storeRejectedAdapter,
                    providerAvailableAdapter,
                    stealRequestAdapter
            ));
        }

        ownershipAssigned = true;
    }

    private Behavior<Message> onBatch(BatchMessage msg) {
        String[][] rows = msg.batch;
        File file = inputFiles[msg.readerId];

        // EOF
        if (rows.length == 0) {
            inputReaderFinished.set(msg.readerId, true);

            // Mark each attribute in that file as complete at ITS OWNER (authoritative)
            int[] attrsInFile = file2attribute.get(file);
            for (int attr : attrsInFile) {
                ActorRef<DataProvider.Message> owner = ownerOf.get(attr);
                if (owner != null) owner.tell(new DataProvider.MarkAttributeComplete(attr));
            }

            return this;
        }

        // transpose rows -> column chunks
        int numRows = rows.length;
        int numCols = rows[0].length;
        int[] attrIds = file2attribute.get(file);

        for (int colIdx = 0; colIdx < numCols; colIdx++) {
            int attr = attrIds[colIdx];
            String[] colChunk = new String[numRows];
            for (int r = 0; r < numRows; r++) {
                colChunk[r] = rows[r][colIdx];
            }
            sendOrBufferPartition(attr, colChunk);
        }

        // pull-next-batch only if NOT paused
        if (!ingestionPaused) {
            inputReaders.get(msg.readerId)
                    .tell(new InputReader.ReadBatchMessage(getContext().getSelf(), batchSize));
        }

        return this;
    }

    private Behavior<Message> onAttributeFullyLoaded(AttributeFullyLoadedMessage msg) {
        int a = msg.attribute;

        if (a >= 0 && a < attributeReady.size() && !attributeReady.getBoolean(a)) {
            attributeReady.set(a, true);
            readyCount++;
        }

        // Start validation only after ALL attributes are ready
        if (!validationPhaseStarted && readyCount == numAttributes) {
            startValidationPhase();
        }
        return this;
    }

    private void startValidationPhase() {
        validationPhaseStarted = true;

        taskQueue.clear();
        totalTasks = 0;
        completedTasks = 0;

        // Generate all ordered pairs lhs != rhs
        for (int lhs = 0; lhs < numAttributes; lhs++) {
            for (int rhs = 0; rhs < numAttributes; rhs++) {
                if (lhs == rhs) continue;
                taskQueue.addLast(pack(lhs, rhs));
                totalTasks++;
            }
        }

        // Kick workers: they will RequestWork
        for (ActorRef<DependencyWorker.Message> w : dependencyWorkers) {
            w.tell(new DependencyWorker.StartValidationMessage());
        }
    }

    private Behavior<Message> onRequestWork(RequestWorkMessage msg) {
        if (!validationPhaseStarted) return this;

        long[] task = taskQueue.pollFirst();
        if (task == null) {
            msg.worker.tell(new DependencyWorker.NoWorkMessage());
            return this;
        }

        int lhs = (int) task[0];
        int rhs = (int) task[1];
        msg.worker.tell(new DependencyWorker.ValidatePairMessage(lhs, rhs));
        return this;
    }

    private Behavior<Message> onGetOwner(GetOwnerMessage msg) {
        ActorRef<DataProvider.Message> owner = ownerOf.get(msg.attribute);
        msg.replyTo.tell(new DependencyWorker.OwnerReplyMessage(msg.attribute, owner));
        return this;
    }

    private Behavior<Message> onValidationResult(ValidationResultMessage msg) {
        completedTasks++;

        if (msg.isIND) {
            // Convert to InclusionDependency immediately; miner holds NO data, just metadata
            InclusionDependency ind = new InclusionDependency(
                    attribute2file[msg.lhs],
                    new String[]{attribute2name[msg.lhs]},
                    attribute2file[msg.rhs],
                    new String[]{attribute2name[msg.rhs]}
            );
            resultCollector.tell(new ResultCollector.ResultMessage(Collections.singletonList(ind)));
        }

        // finish
        if (completedTasks == totalTasks) {
            resultCollector.tell(new ResultCollector.FinalizeMessage());
            master.tell(new Master.MiningFinishedMessage());
        }

        return this;
    }

    private Behavior<Message> onStoreRejected(StoreRejectedMessage msg) {
        // pause ingestion globally (simple & safe)
        if (!ingestionPaused) {
            ingestionPaused = true;
        }
        return this;
    }

    private Behavior<Message> onProviderAvailable(ProviderAvailableMessage msg) {
        // if ANY provider available, attempt to drain pending stores and resume reading
        flushPendingStores();

        if (ingestionPaused && pendingStores.isEmpty()) {
            ingestionPaused = false;
            // resume all readers
            for (int i = 0; i < inputReaders.size(); i++) {
                if (!inputReaderFinished.getBoolean(i)) {
                    inputReaders.get(i).tell(new InputReader.ReadBatchMessage(getContext().getSelf(), batchSize));
                }
            }
        }
        return this;
    }

    private Behavior<Message> onStealRequest(StealColumnRequestMessage msg) {
        // Choose a target provider different from fromProvider
        if (dataProviders.size() < 2) return this;

        ActorRef<DataProvider.Message> from = msg.fromProvider;
        ActorRef<DataProvider.Message> target = null;
        for (ActorRef<DataProvider.Message> p : dataProviders) {
            if (!p.equals(from)) { target = p; break; }
        }
        if (target == null) return this;

        // coordinate: ask FROM to transfer to TARGET
        from.tell(new DataProvider.InitiateSteal(msg.attribute, target));

        // optimistic ownership update happens when target confirms; provider will notify via OwnershipMoved
        // (handled in provider by sending miner a fully loaded message again on the target)
        ownerOf.put(msg.attribute, target);
        return this;
    }

    private Behavior<Message> onShutdown(ShutdownMessage msg) {
        dependencyWorkers.forEach(w -> w.tell(new DependencyWorker.ShutdownMessage()));
        inputReaders.forEach(r -> r.tell(new InputReader.ShutdownMessage()));
        resultCollector.tell(new ResultCollector.ShutdownMessage());
        return Behaviors.stopped();
    }

    ////////////////////
    // Helpers
    ////////////////////

    private void tryStartReading() {
        if (readingStarted) return;
        if (!headersReady) return;
        if (dependencyWorkers.isEmpty()) return;
        if (dataProviders.isEmpty()) return;

        tryAssignOwnership();
        if (!ownershipAssigned) return;

        readingStarted = true;
        startReading();
    }

    private void buildAttributeMappings() {
        int count = 0;
        file2attribute = new HashMap<>();

        for (int i = 0; i < headerLines.length; i++) {
            file2attribute.put(inputFiles[i], new int[headerLines[i].length]);
            count += headerLines[i].length;
        }

        attribute2file = new File[count];
        attribute2name = new String[count];

        int idx = 0;
        for (int i = 0; i < inputFiles.length; i++) {
            for (int j = 0; j < headerLines[i].length; j++) {
                attribute2file[idx] = inputFiles[i];
                attribute2name[idx] = headerLines[i][j];
                file2attribute.get(inputFiles[i])[j] = idx++;
            }
        }

        this.numAttributes = count;
    }

    private void startReading() {
        for (ActorRef<InputReader.Message> r : inputReaders) {
            r.tell(new InputReader.ReadBatchMessage(getContext().getSelf(), batchSize));
        }
    }

    private void sendOrBufferPartition(int attribute, String[] colChunk) {
        if (!ownershipAssigned) {
            pendingStores.addLast(new PendingStore(attribute, colChunk));
            return;
        }

        ActorRef<DataProvider.Message> owner = ownerOf.get(attribute);
        if (owner == null) {
            pendingStores.addLast(new PendingStore(attribute, colChunk));
            return;
        }

        owner.tell(new DataProvider.StorePartition(attribute, colChunk));
    }

    private void flushPendingStores() {
        if (!ownershipAssigned) return;

        int bound = pendingStores.size();
        while (bound-- > 0 && !pendingStores.isEmpty()) {
            PendingStore p = pendingStores.removeFirst();
            ActorRef<DataProvider.Message> owner = ownerOf.get(p.attribute);
            if (owner == null) {
                pendingStores.addLast(p);
                break;
            }
            owner.tell(new DataProvider.StorePartition(p.attribute, p.columnChunk));
        }
    }

    private static long[] pack(int a, int b) {
        return new long[]{a, b};
    }
}