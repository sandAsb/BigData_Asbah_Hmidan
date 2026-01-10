package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import de.ddm.serialization.AkkaSerializable;
import lombok.*;

import java.util.BitSet;

public class DependencyWorkerCameo extends AbstractBehavior<DependencyWorkerCameo.Message> {

    public interface Message extends AkkaSerializable {}

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class StartCameoMessage implements Message {
        int expectedTasks;
        ActorRef<DependencyMiner.Message> miner; // optional: where to forward aggregated results
        int workerId;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class PartialResultMessage implements Message {
        int taskId;
        int[] lhss;
        int[] rhss;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class TaskDoneMessage implements Message {
        int taskId;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {}

    public static final String DEFAULT_NAME = "dependencyWorkerCameo";

    public static Behavior<Message> create() {
        return Behaviors.setup(DependencyWorkerCameo::new);
    }

    private int expected = 0;
    private int done = 0;
    private final BitSet finished = new BitSet();
    private ActorRef<DependencyMiner.Message> miner;
    private int workerId = -1;

    private DependencyWorkerCameo(ActorContext<Message> ctx) {
        super(ctx);
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartCameoMessage.class, this::onStart)
                .onMessage(PartialResultMessage.class, this::onPartial)
                .onMessage(TaskDoneMessage.class, this::onDone)
                .onMessage(ShutdownMessage.class, msg -> Behaviors.stopped())
                .build();
    }

    private Behavior<Message> onStart(StartCameoMessage msg) {
        this.expected = msg.expectedTasks;
        this.miner = msg.miner;
        this.workerId = msg.workerId;
        this.done = 0;
        this.finished.clear();
        return this;
    }

    private Behavior<Message> onPartial(PartialResultMessage msg) {
        // If you still use helpers for local slices somewhere else,
        // this is where you'd forward aggregated results.
        // Example (if you re-introduce worker-side slicing):
        //
        // if (miner != null) miner.tell(new DependencyMiner.ResultMessage(msg.lhss, msg.rhss, workerId));
        //
        return this;
    }

    private Behavior<Message> onDone(TaskDoneMessage msg) {
        if (!finished.get(msg.taskId)) {
            finished.set(msg.taskId);
            done++;
        }

        if (done >= expected) {
            // all tasks accounted for
            return Behaviors.stopped();
        }
        return this;
    }
}