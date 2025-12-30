package de.ddm.actors.profiling;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import de.ddm.actors.Master;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {
    private final Object discoverNaryDependencies;////////////////

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {}

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {} // ADDED FROM A

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HeaderMessage implements Message {
        private static final long serialVersionUID = -5322425954432915838L;
        int readerId;
        String[] header;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Message {
        private static final long serialVersionUID = 4591192372652568030L;
        int readerId;
        String[][] batch;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatusMessage implements Message {
        private static final long serialVersionUID = -5302922080219823523L;
        int workerId;
        boolean active;
        boolean overloaded;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
        ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResultMessage implements Message {
        private static final long serialVersionUID = -7642425159675583598L;
        int[] lhss;
        int[] rhss;
        int workerId;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";

    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService =
            ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create(ActorRef<Master.Message> master) { // ADDED FROM A
        return Behaviors.setup(ctx -> new DependencyMiner(ctx, master));
    }

    private final ActorRef<Master.Message> master; // ADDED FROM A

    private DependencyMiner(ActorContext<Message> context, ActorRef<Master.Message> master) {
        super(context);
        this.master = master;
        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[this.inputFiles.length][];

        this.inputReaders = new ArrayList<>(this.inputFiles.length);
        this.inputReaderFinished = new BooleanArrayList();
        for (int id = 0; id < this.inputFiles.length; id++) {
            this.inputReaders.add(context.spawn(
                    InputReader.create(id, this.inputFiles[id]),
                    InputReader.DEFAULT_NAME + "_" + id));
            this.inputReaderFinished.add(false);
        }

        this.pendingBatchMessages = new LinkedList<>();
        this.pendingRegistrationMessages = new LinkedList<>();

        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = context.spawn(
                LargeMessageProxy.create(context.getSelf().unsafeUpcast(), false),
                LargeMessageProxy.DEFAULT_NAME);

        this.dependencyWorkers = new ArrayList<>();
        this.dependencyWorkerLMPs = new ArrayList<>();
        this.dependencyWorkerActive = new BooleanArrayList();
        this.dependencyWorkerOverloaded = new BooleanArrayList();

        context.getSystem().receptionist()
                .tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    /////////////////
    // Actor State //
    /////////////////

    private static final int batchSize = 10000;

    private long startTime;

    private final File[] inputFiles;
    private final String[][] headerLines;

    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final BooleanList inputReaderFinished;

    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
    private final List<ActorRef<LargeMessageProxy.Message>> dependencyWorkerLMPs;
    private final BooleanList dependencyWorkerActive;
    private final BooleanList dependencyWorkerOverloaded;

    private HashMap<File, int[]> file2attribute;
    private File[] attribute2file;
    private String[] attribute2name;
    private int[] attribute2worker;

    private final List<BatchMessage> pendingBatchMessages;
    private final List<RegistrationMessage> pendingRegistrationMessages;

    private boolean[][] candidateMatrix;
    private List<IntList> worker2attributes;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .onMessage(BatchMessage.class, this::handle)
                .onMessage(HeaderMessage.class, this::handle)
                .onMessage(StatusMessage.class, this::handle)
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(ResultMessage.class, this::handle)
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    ////////////////////////////
    // ADDED CLEAN SHUTDOWN   //
    ////////////////////////////

    private Behavior<Message> handle(ShutdownMessage message) {
        for (ActorRef<DependencyWorker.Message> worker : dependencyWorkers)
            worker.tell(new DependencyWorker.ShutdownMessage());

        for (ActorRef<InputReader.Message> reader : inputReaders)
            reader.tell(new InputReader.ShutdownMessage());

        resultCollector.tell(new ResultCollector.ShutdownMessage());

        return Behaviors.stopped();
    }
    private Behavior<Message> handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
        return this;
    }

    private boolean isHeaderPhase() {
        // The header phase lasts as long as header lines are still missing
        return Arrays.stream(this.headerLines).anyMatch(Objects::isNull);
    }

    private boolean isBatchPhase() {
        // The batch phase lasts as long as input readers are still unfinished and batches are pending
        return !this.isHeaderPhase() &&
                (this.inputReaderFinished.stream().anyMatch(b -> !b) ||
                        !this.pendingBatchMessages.isEmpty());
    }

    private boolean isValidationPhase() {
        // The validation phase lasts until the algorithm ends
        return !this.isBatchPhase();
    }

    private Behavior<Message> handle(HeaderMessage message) {
        this.getContext().getLog().info("HeaderMessage! {} active workers", this.dependencyWorkerActive.stream().filter(b -> b).count());

        // Store the received header lines
        this.headerLines[message.getReaderId()] = message.getHeader();

        // Test if header lines are still missing and in case a header is still missing simply wait
        if (this.isHeaderPhase())
            return this;

        // Create attribute IDs and mappings to and from attribute descriptors, so we can work with IDs during IND discovery
        int numAttributes = 0;
        this.file2attribute = new HashMap<>();
        for (int i = 0; i < this.headerLines.length; i++) {
            file2attribute.put(this.inputFiles[i], new int[this.headerLines[i].length]);
            numAttributes += this.headerLines[i].length;
        }

        this.attribute2file = new File[numAttributes];
        this.attribute2name = new String[numAttributes];
        int attribute = 0;
        for (int i = 0; i < this.inputFiles.length; i++) {
            File file = this.inputFiles[i];
            for (int j = 0; j < this.headerLines[i].length; j++) {
                String name = this.headerLines[i][j];
                this.attribute2file[attribute] = file;
                this.attribute2name[attribute] = name;
                this.file2attribute.get(file)[j] = attribute;
                attribute++;
            }
        }

        // Wait if no workers have registered yet
        if (this.dependencyWorkers.isEmpty())
            return this;

        // Map workers to attributes and start reading
        this.mapAttributesToWorkers();
        this.startReading();

        return this;
    }

    private void mapAttributesToWorkers() {
        int numWorkers = this.dependencyWorkers.size();
        int numAttributes = this.attribute2name.length;

        // Assign attributes to workers with a round-robin strategy so that long and short columns are assigned approximately evenly to workers
        this.attribute2worker = new int[numAttributes];
        for (int attribute = 0; attribute < numAttributes; attribute++)
            this.attribute2worker[attribute] = attribute % numWorkers;
    }

    private void startReading() {
        // Initiate reading of the first batches
        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), batchSize));

        // Inform the dependency workers about their initial responsibilities
        for (int workerId = 0; workerId < this.dependencyWorkers.size(); workerId++) {
            IntArrayList attributes = new IntArrayList();
            for (int attribute = 0; attribute < this.attribute2worker.length; attribute++)
                if (this.attribute2worker[attribute] == workerId)
                    attributes.add(attribute);

            this.dependencyWorkers.get(workerId).tell(new DependencyWorker.InitialAttributesMessage(attributes.toArray(new int[0]), workerId));
            this.dependencyWorkerActive.set(workerId, true);
        }
    }

    private Behavior<Message> handle(BatchMessage message) {
        this.getContext().getLog().info("BatchMessage! {} active workers", this.dependencyWorkerActive.stream().filter(b -> b).count());

        // Close the reading process for this file if the last line was read
        if (message.getBatch().length == 0) {
            this.inputReaderFinished.set(message.getReaderId(), true);

            if (this.isValidationPhase()) {
                // Log the result of the data reading process
                this.getContext().getLog().info("Reading data completed in " + (System.currentTimeMillis() - this.startTime) + "ms");
                for (int attribute = 0; attribute < this.attribute2worker.length; attribute++)
                    this.getContext().getLog().info("Attribute " + attribute + " is on worker " + this.attribute2worker[attribute]);

                // Attempt to initiate the validation phase
                this.initiateValidationPhase();
            }
            return this;
        }

        // Wait for more workers to join and steal if any number of workers is overloaded
        if (this.dependencyWorkerOverloaded.stream().anyMatch(b -> b)) {
            this.pendingBatchMessages.add(message);
            return this;
        }

        // Wait for workers to steal work if workers are currently registering
        if (!this.pendingRegistrationMessages.isEmpty()) {
            this.pendingBatchMessages.add(message);
            return this;
        }

        // Wait for workers to become inactive if one (or more) target workers are currently already active
        File file = this.inputFiles[message.getReaderId()];
        String[][] columns = message.getBatch();
        for (int i = 0; i < columns.length; i++) {
            int attribute = this.file2attribute.get(file)[i];
            int workerId = this.attribute2worker[attribute];
            if (this.dependencyWorkerActive.getBoolean(workerId)) {
                this.pendingBatchMessages.add(message);
                return this;
            }
        }

        // Send the columns and set the corresponding workers to active
        for (int i = 0; i < columns.length; i++) {
            int attribute = this.file2attribute.get(file)[i];
            int workerId = this.attribute2worker[attribute];
            int numColumnsForThisWorker = 0;
            for (int j = 0; j < columns.length; j++)
                if (this.attribute2worker[this.file2attribute.get(file)[j]] == workerId)
                    numColumnsForThisWorker++;

            this.dependencyWorkers.get(workerId).tell(new DependencyWorker.DataMessage(attribute, columns[i], numColumnsForThisWorker));
            this.dependencyWorkerActive.set(workerId, true);
        }

        // Read the next batch from the input reader whose batch we just forwarded
        this.inputReaders.get(message.getReaderId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), batchSize));

        return this;
    }

    private Behavior<Message> handle(StatusMessage message) {
        this.getContext().getLog().info("StatusMessage! {} active workers", this.dependencyWorkerActive.stream().filter(b -> b).count());

        this.dependencyWorkerActive.set(message.getWorkerId(), message.isActive());
        this.dependencyWorkerOverloaded.set(message.getWorkerId(), message.isOverloaded());

        // Check if we are already in validation phase; in that case, StatusMessages are irrelevant, but we might still
        // need to initiate the validation phase, if this was the last active worker
        if (this.isValidationPhase())
            this.initiateValidationPhase();

        // Check all pending registration messages if they can now be processed (if there is any)
        if (!this.pendingRegistrationMessages.isEmpty()) {
            for (RegistrationMessage registrationMessage : this.pendingRegistrationMessages)
                this.getContext().getSelf().tell(registrationMessage);
            this.pendingRegistrationMessages.clear();
            return this;
        }

        // Check all pending batch messages if they can now be processed (if there are any)
        for (BatchMessage batchMessage : this.pendingBatchMessages)
            this.getContext().getSelf().tell(batchMessage);
        this.pendingBatchMessages.clear();
        return this;
    }

    private void registerNewWorker(ActorRef<DependencyWorker.Message> dependencyWorker, ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP) {
        this.dependencyWorkers.add(dependencyWorker);
        this.dependencyWorkerLMPs.add(dependencyWorkerLMP);
        this.getContext().watch(dependencyWorker);
        this.dependencyWorkerActive.add(false);
        this.dependencyWorkerOverloaded.add(false);
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        this.getContext().getLog().info("RegistrationMessage! {} active workers", this.dependencyWorkerActive.stream().filter(b -> b).count());

        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP = message.getDependencyWorkerLMP();

        // Ignore the message if we already know this worker
        if (this.dependencyWorkers.contains(dependencyWorker))
            return this;

        // Accept the registration and simply wait if we are still in the header phase; work has not been assigned to workers yet
        if (this.isHeaderPhase()) {
            this.registerNewWorker(dependencyWorker, dependencyWorkerLMP);

            return this;
        }

        if (this.isBatchPhase()) {
            // Accept the registration and start the data reading process, if this is our first worker in batch phase
            if (this.dependencyWorkers.isEmpty()) {
                this.registerNewWorker(dependencyWorker, dependencyWorkerLMP);

                this.mapAttributesToWorkers();
                this.startReading();
                return this;
            }

            // Accept the registration and let the worker steal columns, if all workers are available i.e. no batches are being processed or stolen
            if (this.dependencyWorkerActive.stream().noneMatch(b -> b)) {
                this.registerNewWorker(dependencyWorker, dependencyWorkerLMP);

                this.stealAttributesTo(dependencyWorker, this.dependencyWorkers.indexOf(dependencyWorker), dependencyWorkerLMP);
                return this;
            }
        }

        // Reject the worker, if we are already in validation phase; it does not make sense to steal columns at this point
        // Note: The validation strategy implemented in this algorithm assumes that the worker set does not change in the
        // 		 validation phase, which means that no new workers join the validation and no workers fail.
        if (this.isValidationPhase())
            return this;

        // Put the registration to pending queue, if we cannot register the worker yet
        this.pendingRegistrationMessages.add(message);
        return this;
    }

    private void stealAttributesTo(ActorRef<DependencyWorker.Message> worker, int workerId, ActorRef<LargeMessageProxy.Message> workerLMP) {
        // Find attributes to steal; we aim to steal evenly from the workers with the most partitions (this is a very simple stealing approach)
        IntSet attributesToSteal = new IntAVLTreeSet();
        int numWorkers = this.dependencyWorkers.size();
        int numAttributes = this.attribute2name.length;
        int numAttributesToSteal = (int) Math.floor((double) numAttributes / numWorkers);

        int[] attributesPerWorkerCounts = new int[numWorkers];
        for (int wId : this.attribute2worker)
            attributesPerWorkerCounts[wId] += 1;

        Random random = new Random();
        while (attributesToSteal.size() < numAttributesToSteal) {
            // Find worker with the currently most partitions
            int workerWithHighestCount = 0;
            int highestCount = attributesPerWorkerCounts[0];
            for (int i = 1; i < numWorkers; i++) {
                if (attributesPerWorkerCounts[i] > highestCount) {
                    workerWithHighestCount = i;
                    highestCount = attributesPerWorkerCounts[i];
                }
            }

            // Steal one random partition from the worker with the currently most partitions
            int randomPartitionNumber = random.nextInt(highestCount) + 1;
            for (int i = 0; i < this.attribute2worker.length; i++) {
                if ((this.attribute2worker[i] == workerWithHighestCount) && (!attributesToSteal.contains(i))) {
                    randomPartitionNumber--;
                    if (randomPartitionNumber == 0) {
                        attributesToSteal.add(i);
                        break;
                    }
                }
            }
            attributesPerWorkerCounts[workerWithHighestCount] -= 1;
        }

        // Collect the stolen attributes for every worker that needs to send stolen attributes to the new worker and reassign stolen attributes
        Int2ObjectMap<IntArrayList> worker2stolenAttributes = new Int2ObjectArrayMap<>(this.dependencyWorkers.size());
        for (int attribute : attributesToSteal) {
            int bestolenWorkerId = this.attribute2worker[attribute];
            if (!worker2stolenAttributes.containsKey(bestolenWorkerId))
                worker2stolenAttributes.put(bestolenWorkerId, new IntArrayList());
            worker2stolenAttributes.get(bestolenWorkerId).add(attribute);

            this.attribute2worker[attribute] = workerId;
        }

        // Send the MoveDataMessages to start the stealing process
        for (Int2ObjectMap.Entry<IntArrayList> entry : worker2stolenAttributes.int2ObjectEntrySet()) {
            int[] attributes = entry.getValue().toArray(new int[0]);
            int bestolenWorkerId = entry.getIntKey();
            DependencyWorker.MoveDataMessage moveDataMessage = new DependencyWorker.MoveDataMessage(
                    attributes, worker, workerId, workerLMP, worker2stolenAttributes.size());
            this.dependencyWorkers.get(bestolenWorkerId).tell(moveDataMessage);
            this.dependencyWorkerActive.set(bestolenWorkerId, true);
        }
        this.dependencyWorkerActive.set(workerId, true);
    }

    private void initiateValidationPhase() {
        int numAttributes = this.attribute2name.length;
        int numWorkers = this.dependencyWorkers.size();

        // Check that all workers are inactive; otherwise, we wait for a status update of a still active worker to initiate the validation phase
        if (this.dependencyWorkerActive.stream().anyMatch(b -> b))
            return;

        // Create an IND candidate matrix to keep track of the candidate validations
        this.candidateMatrix = new boolean[numAttributes][];
        for (int i = 0; i < numAttributes; i++)
            this.candidateMatrix[i] = new boolean[numAttributes];

        // Create a worker-to-attributes mapping to support the initial candidate generation
        this.worker2attributes = new ArrayList<>(numWorkers);
        for (int workerId = 0; workerId < numWorkers; workerId++)
            this.worker2attributes.add(new IntArrayList((int) Math.ceil((float) numAttributes / (float) numWorkers)));
        for (int attribute = 0; attribute < numAttributes; attribute++)
            this.worker2attributes.get(this.attribute2worker[attribute]).add(attribute);

        // Send a ValidateLocalCandidatesMessage to all workers and mark their candidates as validated. The workers will
        // then receive further validation tasks via work pulling, i.e., whenever they report results.
        for (int workerId = 0; workerId < numWorkers; workerId++) {
            this.dependencyWorkers.get(workerId).tell(new DependencyWorker.ValidateLocalCandidatesMessage());
            IntList workerAttributes = this.worker2attributes.get(workerId);
            for (int i = 0; i < workerAttributes.size(); i++) {
                for (int j = i; j < workerAttributes.size(); j++) {
                    this.candidateMatrix[workerAttributes.getInt(i)][workerAttributes.getInt(j)] = true;
                    this.candidateMatrix[workerAttributes.getInt(j)][workerAttributes.getInt(i)] = true;
                }
            }

            this.dependencyWorkerActive.set(workerId, true);
        }
    }

    private Behavior<Message> handle(ResultMessage message) {
        this.getContext().getLog().info("ResultMessage! {} INDs from worker {}", message.getLhss().length, message.getWorkerId());

        // Create Inclusion Dependency objects from the results
        List<InclusionDependency> inds = new ArrayList<>(message.getLhss().length);
        for (int i = 0; i < message.getLhss().length; i++) {
            String[] dependentAttributes = {this.attribute2name[message.getLhss()[i]]};
            String[] referencedAttributes = {this.attribute2name[message.getRhss()[i]]};
            inds.add(new InclusionDependency(this.attribute2file[message.getLhss()[i]], dependentAttributes, this.attribute2file[message.getRhss()[i]], referencedAttributes));
        }

        // Send Inclusion Dependency objects to the result collector
        this.resultCollector.tell(new ResultCollector.ResultMessage(inds));

        // Find more work for the dependency worker
        int workerId = message.getWorkerId();
        for (int attribute : this.worker2attributes.get(workerId)) {
            for (int otherAttribute = 0; otherAttribute < this.attribute2name.length; otherAttribute++) {
                if (!this.candidateMatrix[attribute][otherAttribute]) {
                    // Collect all the worker's attributes that still require validation with the current other attribute and mark them validated
                    IntList untestedAttributes = new IntArrayList(this.worker2attributes.get(workerId).size());
                    for (int untestedAttributeCandidate : this.worker2attributes.get(workerId)) {
                        if (!this.candidateMatrix[otherAttribute][untestedAttributeCandidate]) {
                            this.candidateMatrix[otherAttribute][untestedAttributeCandidate] = true;
                            this.candidateMatrix[untestedAttributeCandidate][otherAttribute] = true;
                            untestedAttributes.add(untestedAttributeCandidate);
                        }
                    }

                    // Tell the owner of the other attribute to send that attribute's column to the worker for validation
                    int otherWorkerId = this.attribute2worker[otherAttribute];
                    this.dependencyWorkers.get(otherWorkerId).tell(new DependencyWorker.ShareForValidationMessage(
                            otherAttribute, untestedAttributes.toIntArray(), this.dependencyWorkers.get(workerId),
                            workerId, this.dependencyWorkerLMPs.get(workerId)));

                    return this;
                }
            }
        }

        // Set the worker to inactive, because no further validation work could be found for its attributes
        this.dependencyWorkerActive.set(workerId, false);

        // End the program if all INDs have been checked, which is, if all workers are inactive now
        if (this.dependencyWorkerActive.stream().noneMatch(b -> b))
            this.end();

        return this;
    }
    private void end() {
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);

        master.tell(new Master.MiningFinishedMessage());
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        this.dependencyWorkers.remove(dependencyWorker);
        return this;
    }
}
