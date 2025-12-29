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
import de.ddm.actors.Master;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

/**
 * Central coordinator:
 *  - receives headers & batches from InputReaders
 *  - stores all column values in memory
 *  - when all input is read, mines unary INDs locally
 *  - sends results to ResultCollector and notifies Master
 */
public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage { }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HeaderMessage implements Message {
        private static final long serialVersionUID = -5322425954432915838L;
        int id;           // file id
        String[] header;  // header line for that file
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Message {
        private static final long serialVersionUID = 4591192372652568030L;
        int id;                 // file id
        List<String[]> batch;   // batch of rows
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegistrationMessage implements Message {
        private static final long serialVersionUID = -4025238529984914107L;
        ActorRef<DependencyWorker.Message> dependencyWorker;
    }
    @NoArgsConstructor
    public static class ShutdownMessage implements Message { }


    /////////////////////////////
    // Helper: Column identity //
    /////////////////////////////

    @Getter
    @AllArgsConstructor
    public static class ColumnId {
        private final int fileId;
        private final int columnIndex;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ColumnId)) return false;
            ColumnId that = (ColumnId) o;
            return fileId == that.fileId && columnIndex == that.columnIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileId, columnIndex);
        }

        @Override
        public String toString() {
            return "file" + fileId + ".col" + columnIndex;
        }
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dependencyMiner";
    public static final ServiceKey<DependencyMiner.Message> dependencyMinerService =
            ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

    public static Behavior<Message> create(ActorRef<Master.Message> master) {
        return Behaviors.setup(ctx -> new DependencyMiner(ctx, master));
    }

    private DependencyMiner(ActorContext<Message> context, ActorRef<Master.Message> master) {
        super(context);

        this.master = master;

        this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
        this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
        this.headerLines = new String[this.inputFiles.length][];
        this.finishedReading = new boolean[this.inputFiles.length];

        this.inputReaders = new ArrayList<>(inputFiles.length);
        for (int id = 0; id < this.inputFiles.length; id++) {
            this.inputReaders.add(
                    context.spawn(
                            InputReader.create(id, this.inputFiles[id]),
                            InputReader.DEFAULT_NAME + "_" + id
                    )
            );
        }

        this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
        this.largeMessageProxy = context.spawn(
                LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
                LargeMessageProxy.DEFAULT_NAME
        );

        this.dependencyWorkers = new ArrayList<>();

        // in-memory structures
        this.columnValues = new HashMap<>();
        this.allColumns = new ArrayList<>();

        // reading coordination
        this.startedReading = false;
        this.minWorkersToStart = 1;

        context.getSystem().receptionist()
                .tell(Receptionist.register(dependencyMinerService, context.getSelf()));
    }

    ///////////////////
    // Actor State   //
    ///////////////////

    private final ActorRef<Master.Message> master;

    private long startTime;

    private final boolean discoverNaryDependencies; // currently unused; unary only
    private final File[] inputFiles;
    private final String[][] headerLines;
    private final boolean[] finishedReading;

    private final List<ActorRef<InputReader.Message>> inputReaders;
    private final ActorRef<ResultCollector.Message> resultCollector;
    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

    // new: in-memory column storage
    private final Map<ColumnId, Set<String>> columnValues;
    private final List<ColumnId> allColumns;

    // wait for at least one worker
    private final int minWorkersToStart;
    private boolean startedReading;

    //////////////////////
    // Actor Behavior   //
    //////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(HeaderMessage.class, this::handle)
                .onMessage(BatchMessage.class, this::handle)
                .onMessage(RegistrationMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)   // 👈 add this
                .onSignal(Terminated.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        tryStartReading();
        return this;
    }

    private void tryStartReading() {
        if (this.startedReading)
            return;

        if (this.dependencyWorkers.size() < this.minWorkersToStart) {
            this.getContext().getLog().info(
                    "Waiting for workers before reading input... {}/{} registered",
                    this.dependencyWorkers.size(),
                    this.minWorkersToStart
            );
            return;
        }

        this.startedReading = true;
        this.getContext().getLog().info(
                "Enough workers registered ({}). Starting input reading.",
                this.dependencyWorkers.size()
        );

        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));

        for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
            inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));
    }

    private Behavior<Message> handle(HeaderMessage message) {
        int fileId = message.getId();
        String[] header = message.getHeader();
        this.headerLines[fileId] = header;

        // register all columns for this file
        for (int colIndex = 0; colIndex < header.length; colIndex++) {
            ColumnId colId = new ColumnId(fileId, colIndex);
            if (!columnValues.containsKey(colId)) {
                columnValues.put(colId, new HashSet<>());
                allColumns.add(colId);
            }
        }

        return this;
    }

    private Behavior<Message> handle(BatchMessage message) {
        int fileId = message.getId();
        List<String[]> batch = message.getBatch();

        if (batch.isEmpty()) {
            this.finishedReading[fileId] = true;

            if (allFinished()) {
                this.getContext().getLog().info("All input files read, starting IND mining...");
                mineUnaryINDsLocally();
                endAndShutdown();
            }
            return this;
        }

        // store values per column
        for (String[] row : batch) {
            if (row == null) continue;
            int len = row.length;
            for (int colIndex = 0; colIndex < len; colIndex++) {
                ColumnId colId = new ColumnId(fileId, colIndex);
                String value = row[colIndex];
                columnValues
                        .computeIfAbsent(colId, k -> new HashSet<>())
                        .add(value);
            }
        }

        // request next batch for this file
        this.inputReaders.get(fileId)
                .tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), 10000));

        return this;
    }

    private Behavior<Message> handle(RegistrationMessage message) {
        ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
        if (!this.dependencyWorkers.contains(dependencyWorker)) {
            this.dependencyWorkers.add(dependencyWorker);
            this.getContext().watch(dependencyWorker);
            tryStartReading();
        }
        return this;
    }

    private Behavior<Message> handle(Terminated signal) {
        ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
        this.dependencyWorkers.remove(dependencyWorker);
        return this;
    }

    private Behavior<Message> handle(ShutdownMessage message) {

        // Forward shutdown to workers
        for (ActorRef<DependencyWorker.Message> worker : dependencyWorkers) {
            worker.tell(new DependencyWorker.ShutdownMessage());
        }

        // Forward shutdown to input readers
        for (ActorRef<InputReader.Message> reader : inputReaders) {
            reader.tell(new InputReader.ShutdownMessage());
        }

        // Forward shutdown to result collector
        resultCollector.tell(new ResultCollector.ShutdownMessage());

        return Behaviors.stopped();
    }

    /////////////////////////////
    // Mining & Shutdown Logic //
    /////////////////////////////

    private boolean allFinished() {
        for (boolean b : this.finishedReading)
            if (!b) return false;
        return true;
    }

    /**
     * Very simple unary IND mining: for every ordered column pair (A,B),
     * check whether values(A) is a subset of values(B).
     * This is centralized and will not scale to huge data, but is correct.
     */
    private void mineUnaryINDsLocally() {
        this.getContext().getLog().info("Mining unary INDs locally over {} columns...", allColumns.size());

        List<InclusionDependency> discovered = new ArrayList<>();

        for (ColumnId dep : allColumns) {
            Set<String> depValues = columnValues.get(dep);
            if (depValues == null || depValues.isEmpty())
                continue;

            for (ColumnId ref : allColumns) {
                if (dep.equals(ref)) continue;

                Set<String> refValues = columnValues.get(ref);
                if (refValues == null || refValues.isEmpty())
                    continue;

                if (isSubset(depValues, refValues)) {
                    // Build InclusionDependency using File + attribute name array
                    File depFile = inputFiles[dep.getFileId()];
                    File refFile = inputFiles[ref.getFileId()];

                    String depAttr = headerLines[dep.getFileId()][dep.getColumnIndex()];
                    String refAttr = headerLines[ref.getFileId()][ref.getColumnIndex()];

                    InclusionDependency ind = new InclusionDependency(
                            depFile,
                            new String[]{depAttr},
                            refFile,
                            new String[]{refAttr}
                    );
                    discovered.add(ind);
                }
            }
        }

        this.getContext().getLog().info("Discovered {} unary INDs.", discovered.size());

        if (!discovered.isEmpty()) {
            this.resultCollector.tell(new ResultCollector.ResultMessage(discovered));
        }
    }

    private boolean isSubset(Set<String> left, Set<String> right) {
        for (String v : left) {
            if (!right.contains(v)) {
                return false;
            }
        }
        return true;
    }

    private void endAndShutdown() {
        this.resultCollector.tell(new ResultCollector.FinalizeMessage());
        long discoveryTime = System.currentTimeMillis() - this.startTime;
        this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);

        this.master.tell(new Master.MiningFinishedMessage());
    }
}
