package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.utils.MemoryUtils;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -5246338806092216222L;
        Receptionist.Listing listing;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InitialAttributesMessage implements Message {
        private static final long serialVersionUID = -7816276125448176767L;
        int[] attributes;
        int workerId;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataMessage implements Message {
        private static final long serialVersionUID = 8586787707165511461L;
        int attribute;
        String[] column;
        int messagesToAccept;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MoveDataMessage implements Message {
        private static final long serialVersionUID = -1382228729107690967L;
        int[] attributes;
        ActorRef<DependencyWorker.Message> toWorker;
        int toWorkerId;
        ActorRef<LargeMessageProxy.Message> toWorkerLMP;
        int moveMessages;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AcceptDataMessage implements Message {
        private static final long serialVersionUID = -7984837187618402770L;
        Map<Integer, Set<String>> columns;
        int messagesToAccept;
        int workerId;
    }

    @Getter
    @NoArgsConstructor
    public static class ValidateLocalCandidatesMessage implements Message {
        private static final long serialVersionUID = -2765506199949499328L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShareForValidationMessage implements Message {
        private static final long serialVersionUID = -1228839983943869503L;
        int attribute;
        int[] untestedAttributes;
        ActorRef<DependencyWorker.Message> toWorker;
        int toWorkerId;
        ActorRef<LargeMessageProxy.Message> toWorkerLMP;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidateCandidatesMessage implements Message {
        private static final long serialVersionUID = -524894512354313788L;
        int attribute;
        List<String> column;
        int[] untestedAttributes;
        int workerId;
    }
    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 1L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorker";

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyWorker::new);
    }

    private DependencyWorker(ActorContext<Message> context) {
        super(context);

        final ActorRef<Receptionist.Listing> listingResponseAdapter =
                context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist()
                .tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

        this.largeMessageProxy =
                this.getContext().spawn(
                        LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
                        LargeMessageProxy.DEFAULT_NAME);

        // We will use 40% of the available free memory for storing column data
        this.availableMemory =
                (long) (0.4 * (Math.max(MemoryUtils.bytesMax(), MemoryUtils.bytesCommitted()) - MemoryUtils.bytesUsed()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    private List<ActorRef<DependencyWorkerHelper.Message>> dependencyWorkerHelpers;

    private ActorRef<DependencyMiner.Message> dependencyMiner;
    private int workerId = -1;

    private final long availableMemory;
    private long usedMemoryMeasured;
    private long usedMemoryPredicted;

    private Int2ObjectMap<Set<String>> columns = new Int2ObjectArrayMap<>();
    private Int2ObjectMap<List<String>> sortedColumns;
    private long valueCount;
    private double avgValueSize;
    private final static long valueCountWithoutMemoryMeasurement = 100000;

    private int acceptMessageCounter = 0;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(InitialAttributesMessage.class, this::handle)
                .onMessage(DataMessage.class, this::handle)
                .onMessage(MoveDataMessage.class, this::handle)
                .onMessage(AcceptDataMessage.class, this::handle)
                .onMessage(ValidateLocalCandidatesMessage.class, this::handle)
                .onMessage(ShareForValidationMessage.class, this::handle)
                .onMessage(ValidateCandidatesMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        if (this.dependencyMiner != null)
            return this;

        Set<ActorRef<DependencyMiner.Message>> dependencyMiners =
                message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
        if (!dependencyMiners.isEmpty() && (this.dependencyMiner == null)) {
            this.dependencyMiner = dependencyMiners.iterator().next();
            this.dependencyMiner.tell(
                    new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
        }
        return this;
    }
    private void reportStatus() {
        // Calculate memory exhaustion
        boolean overloaded = this.availableMemory < this.usedMemoryPredicted;

        // Answer with status
        this.dependencyMiner.tell(new DependencyMiner.StatusMessage(this.workerId, false, overloaded));

        this.getContext().getLog().info("Status: overloaded = {} ({} / {})", overloaded, this.availableMemory / 1024 / 1024, this.usedMemoryPredicted / 1024 / 1024);
    }

    private Behavior<Message> handle(InitialAttributesMessage message) {
        // Initialize this worker's ID
        this.workerId = message.getWorkerId();

        // Initialize the column sets of attributes that this worker is responsible for
        for (int attribute : message.getAttributes())
            this.columns.put(attribute, new HashSet<>());

        this.reportStatus();
        return this;
    }

    private Behavior<Message> handle(DataMessage message) {
        // Store data and update value counts
        Set<String> localColumn = this.columns.get(message.getAttribute());
        long columnValueCountBefore = localColumn.size();
        localColumn.addAll(Arrays.asList(message.getColumn()));
        long columnValueCountAfter = localColumn.size();
        this.valueCount = this.valueCount - columnValueCountBefore + columnValueCountAfter;

        // Update memory usage
        this.updateMemoryUsage();

        // Update acceptMessageCounter and test if this was the last message to be accepted, i.e., the batch has arrived
        this.acceptMessageCounter++;
        if (this.acceptMessageCounter == message.getMessagesToAccept()) {
            this.acceptMessageCounter = 0;

            this.reportStatus();
        }
        return this;
    }

    private void updateMemoryUsage() {
        // Skip any memory measurement if the value count is and always was small
        if ((this.valueCount < valueCountWithoutMemoryMeasurement) && (this.usedMemoryMeasured == 0))
            return;

        // Measure memory consumption if the memory was never measured before
        if (this.usedMemoryMeasured == 0) {
            this.usedMemoryMeasured = MemoryUtils.byteSizeOf(this.columns);
            this.usedMemoryPredicted = this.usedMemoryMeasured;
            this.avgValueSize = (double) this.usedMemoryMeasured / (double) this.valueCount;
            return;
        }

        // Predict the memory consumption
        this.usedMemoryPredicted = (long) Math.ceil(this.valueCount * this.avgValueSize);
        return;
    }

    private Behavior<Message> handle(MoveDataMessage message) {
        // Extract local columns that should be shared
        Map<Integer, Set<String>> stolenColumns = new HashMap<>();
        for (int attribute : message.getAttributes()) {
            Set<String> stolenColumn = this.columns.remove(attribute);
            stolenColumns.put(attribute, stolenColumn);
            this.valueCount = this.valueCount - stolenColumn.size();
        }

        // Update memory usage
        this.updateMemoryUsage();

        // Send columns to target worker
        LargeMessageProxy.LargeMessage acceptDataMessage = new AcceptDataMessage(stolenColumns, message.getMoveMessages(), message.getToWorkerId());
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(acceptDataMessage, message.getToWorkerLMP()));

        this.reportStatus();
        return this;
    }

    private Behavior<Message> handle(AcceptDataMessage message) {
        // Initialize this worker's ID
        this.workerId = message.getWorkerId();

        // Accept the columns
        for (Map.Entry<Integer, Set<String>> entry : message.getColumns().entrySet()) {
            this.columns.put(entry.getKey().intValue(), entry.getValue());
            this.valueCount = this.valueCount - entry.getValue().size();
        }

        // Update memory usage
        this.updateMemoryUsage();

        // Update acceptMessageCounter and test if this was the last message to be accepted, i.e., the stealing is done
        this.acceptMessageCounter++;
        if (this.acceptMessageCounter == message.getMessagesToAccept()) {
            this.acceptMessageCounter = 0;

            this.reportStatus();
        }
        return this;
    }

    private void initializeValidation() {
        // Return if validation has already been initiated
        if (this.sortedColumns != null)
            return;

        // Sort all columns for more effective IND candidate validation
        this.sortColumns();

        // Spawn helper actors to perform the actual candidate validations
        this.spawnHelperActors();
    }

    private void sortColumns() {
        this.sortedColumns = new Int2ObjectArrayMap<>(this.columns.size());
        IntList attributes = new IntArrayList(this.columns.keySet());
        for (int attribute : attributes) {
            Set<String> column = this.columns.remove(attribute);
            List<String> sortedColumn = new ArrayList<>(column);
            Collections.sort(sortedColumn);
            this.sortedColumns.put(attribute, sortedColumn);
        }
        this.columns = null;
    }

    private void spawnHelperActors() {
        final int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();

        // Spawn a cameo actor for the helpers to externalize the combination of the results for miner
        ActorRef<DependencyWorkerCameo.Message> cameo = this.getContext().spawn(
                DependencyWorkerCameo.create(this.dependencyMiner, numWorkers, this.workerId),
                DependencyWorkerCameo.DEFAULT_NAME,
                DispatcherSelector.fromConfig("akka.worker-pool-dispatcher"));

        // Spawn the helper actors
        this.dependencyWorkerHelpers = new ArrayList<>(numWorkers);
        for (int id = 0; id < numWorkers; id++)
            this.dependencyWorkerHelpers.add(this.getContext().spawn(
                    DependencyWorkerHelper.create(this.sortedColumns, cameo),
                    DependencyWorkerHelper.DEFAULT_NAME + "_" + id,
                    DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")));

    }

    private Behavior<Message> handle(ValidateLocalCandidatesMessage message) {
        this.getContext().getLog().info("Validating all local INDs");

        this.initializeValidation();

        // Initialize the task messages for this validation round
        List<DependencyWorkerHelper.ValidateLocalTaskMessage> tasks = new ArrayList<>(this.dependencyWorkerHelpers.size());
        for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
            tasks.add(new DependencyWorkerHelper.ValidateLocalTaskMessage(new IntArrayList(), new IntArrayList()));

        // Form all pairs of local attributes and assign them to the helper's tasks in a round-robin fashion
        int nextHelper = 0;
        int[] attributes = this.sortedColumns.keySet().toArray(new int[0]);
        for (int i = 0; i < attributes.length - 1; i++) {
            for (int j = i + 1; j < attributes.length; j++) {
                tasks.get(nextHelper).getAttributes1().add(attributes[i]);
                tasks.get(nextHelper).getAttributes2().add(attributes[j]);
                nextHelper = (nextHelper == tasks.size() - 1) ? 0 : nextHelper + 1;
            }
        }

        // Send all validation tasks to the corresponding helpers
        for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
            this.dependencyWorkerHelpers.get(i).tell(tasks.get(i));

        return this;
    }

    private Behavior<Message> handle(ShareForValidationMessage message) {
        this.initializeValidation();

        // Send the local column to the remote worker for IND candidate validation
        LargeMessageProxy.LargeMessage validateCandidatesMessage = new ValidateCandidatesMessage(
                message.getAttribute(), this.sortedColumns.get(message.getAttribute()), message.getUntestedAttributes(), message.getToWorkerId());
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(validateCandidatesMessage, message.getToWorkerLMP()));
        return this;
    }

    private Behavior<Message> handle(ValidateCandidatesMessage message) {
        this.getContext().getLog().info("Validating " + message.getUntestedAttributes().length + " INDs");

        this.initializeValidation();

        // Initialize the task messages for this validation round
        List<DependencyWorkerHelper.ValidateTaskMessage> tasks = new ArrayList<>(this.dependencyWorkerHelpers.size());
        for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
            tasks.add(new DependencyWorkerHelper.ValidateTaskMessage(message.getAttribute(), message.getColumn(), new IntArrayList()));

        // Assign untested local columns to the helper's tasks in a round-robin fashion
        int nextHelper = 0;
        for (int localAttribute : message.getUntestedAttributes()) {
            tasks.get(nextHelper).getLocalAttributes().add(localAttribute);
            nextHelper = (nextHelper == tasks.size() - 1) ? 0 : nextHelper + 1;
        }

        // Send all validation tasks to the corresponding helpers
        for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
            this.dependencyWorkerHelpers.get(i).tell(tasks.get(i));

        return this;
    }
    private Behavior<Message> handle(ShutdownMessage message) {
        this.getContext().getLog().info("DependencyWorker shutting down cleanly.");
        return Behaviors.stopped();
    }
}
