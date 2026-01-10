package de.ddm.actors.profiling;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import lombok.*;

import java.util.*;

/**
 * Pull-based global validator:
 * - Worker does NOT own attributes
 * - Worker pulls tasks from miner (RequestWork)
 * - For each task (lhs,rhs), worker fetches columns from authoritative owners and validates lhs ⊆ rhs
 */
public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

    public static final String DEFAULT_NAME = "dependencyWorker";

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {}

    // ---- lifecycle ----
    @NoArgsConstructor public static class ShutdownMessage implements Message {}

    // Miner receptionist
    @Getter @AllArgsConstructor @NoArgsConstructor
    public static class MinerListingMessage implements Message {
        Receptionist.Listing listing;
    }

    // Miner -> Worker
    @NoArgsConstructor public static class StartValidationMessage implements Message {}
    @NoArgsConstructor public static class NoWorkMessage implements Message {}

    @Getter @AllArgsConstructor @NoArgsConstructor
    public static class ValidatePairMessage implements Message {
        int lhs;
        int rhs;
    }

    // Miner -> Worker owner reply
    @Getter @AllArgsConstructor @NoArgsConstructor
    public static class OwnerReplyMessage implements Message {
        int attribute;
        ActorRef<DataProvider.Message> owner;
    }

    // Provider -> Worker column reply (adapter)
    @Getter @AllArgsConstructor @NoArgsConstructor
    public static class ColumnMessage implements Message {
        int attribute;
        String[] column;
    }

    public static Behavior<Message> create(ActorRef<DataProvider.Message> provider) {
        return Behaviors.setup(DependencyWorker::new);
    }

    private ActorRef<DependencyMiner.Message> miner;

    // per-task state
    private int curLhs = -1;
    private int curRhs = -1;
    private ActorRef<DataProvider.Message> lhsOwner;
    private ActorRef<DataProvider.Message> rhsOwner;
    private String[] lhsColumn;
    private String[] rhsColumn;

    private final ActorRef<DataProvider.ResponsePartition> providerColumnAdapter;

    private DependencyWorker(ActorContext<Message> ctx) {
        super(ctx);

        // Subscribe to miner
        ActorRef<Receptionist.Listing> listingAdapter =
                ctx.messageAdapter(Receptionist.Listing.class, MinerListingMessage::new);

        ctx.getSystem().receptionist().tell(
                Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingAdapter)
        );

        // Provider column adapter
        this.providerColumnAdapter =
                ctx.messageAdapter(DataProvider.ResponsePartition.class,
                        r -> new ColumnMessage(r.attribute, r.column));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(MinerListingMessage.class, this::onMinerListing)
                .onMessage(StartValidationMessage.class, this::onStartValidation)
                .onMessage(ValidatePairMessage.class, this::onValidatePair)
                .onMessage(OwnerReplyMessage.class, this::onOwnerReply)
                .onMessage(ColumnMessage.class, this::onColumn)
                .onMessage(NoWorkMessage.class, this::onNoWork)
                .onMessage(ShutdownMessage.class, msg -> Behaviors.stopped())
                .build();
    }

    private Behavior<Message> onMinerListing(MinerListingMessage msg) {
        Set<ActorRef<DependencyMiner.Message>> miners =
                msg.listing.getServiceInstances(DependencyMiner.dependencyMinerService);

        if (!miners.isEmpty() && this.miner == null) {
            this.miner = miners.iterator().next();
            this.miner.tell(new DependencyMiner.RegistrationMessage(getContext().getSelf(), null));
        }
        return this;
    }

    private Behavior<Message> onStartValidation(StartValidationMessage msg) {
        if (miner == null) return this;
        miner.tell(new DependencyMiner.RequestWorkMessage(getContext().getSelf()));
        return this;
    }

    private Behavior<Message> onValidatePair(ValidatePairMessage msg) {
        this.curLhs = msg.lhs;
        this.curRhs = msg.rhs;

        // reset per-task state
        this.lhsOwner = null;
        this.rhsOwner = null;
        this.lhsColumn = null;
        this.rhsColumn = null;

        // ask miner for authoritative owners
        miner.tell(new DependencyMiner.GetOwnerMessage(curLhs, getContext().getSelf()));
        miner.tell(new DependencyMiner.GetOwnerMessage(curRhs, getContext().getSelf()));
        return this;
    }

    private Behavior<Message> onOwnerReply(OwnerReplyMessage msg) {
        if (msg.attribute == curLhs) lhsOwner = msg.owner;
        if (msg.attribute == curRhs) rhsOwner = msg.owner;

        // once both owners known, request columns
        if (lhsOwner != null && rhsOwner != null) {
            lhsOwner.tell(new DataProvider.RequestPartition(curLhs, providerColumnAdapter));
            rhsOwner.tell(new DataProvider.RequestPartition(curRhs, providerColumnAdapter));
        }
        return this;
    }

    private Behavior<Message> onColumn(ColumnMessage msg) {
        if (msg.attribute == curLhs) lhsColumn = msg.column;
        if (msg.attribute == curRhs) rhsColumn = msg.column;

        if (lhsColumn != null && rhsColumn != null) {
            boolean ok = isSubsetDistinct(lhsColumn, rhsColumn);

            miner.tell(new DependencyMiner.ValidationResultMessage(curLhs, curRhs, ok));

            // pull next task
            miner.tell(new DependencyMiner.RequestWorkMessage(getContext().getSelf()));
        }
        return this;
    }

    private Behavior<Message> onNoWork(NoWorkMessage msg) {
        // no more tasks; worker stays alive
        return this;
    }

    /**
     * Validates lhs ⊆ rhs in a memory-friendly way:
     * - build a set of RHS values
     * - check all LHS values are contained
     * You can replace with sorted-distinct two-pointer if you already enforce sorting.
     */
    private boolean isSubsetDistinct(String[] lhs, String[] rhs) {
        // Treat null as empty
        if (lhs == null || lhs.length == 0) return true;
        if (rhs == null || rhs.length == 0) return false;

        // Build RHS set
        Set<String> rhsSet = new ObjectOpenHashSet<>(rhs.length * 2);
        for (String v : rhs) if (v != null) rhsSet.add(v);

        for (String v : lhs) {
            if (v == null) continue;
            if (!rhsSet.contains(v)) return false;
        }
        return true;
    }
}