package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.serialization.AkkaSerializable;
import lombok.NoArgsConstructor;

public class Master extends AbstractBehavior<Master.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable { }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 7516129288777469221L;
    }

    /** Sent by DependencyMiner when the discovery has finished and results were finalized. */
    @NoArgsConstructor
    public static class MiningFinishedMessage implements Message {
        private static final long serialVersionUID = 1L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Behavior<Message> create(ActorRef<Guardian.Message> guardian) {
        return Behaviors.setup(ctx -> new Master(ctx, guardian));
    }

    private Master(ActorContext<Message> context, ActorRef<Guardian.Message> guardian) {
        super(context);
        Reaper.watchWithDefaultReaper(this.getContext().getSelf());

        this.guardian = guardian;

        this.dependencyMiner = context.spawn(
                DependencyMiner.create(this.getContext().getSelf()),
                DependencyMiner.DEFAULT_NAME,
                DispatcherSelector.fromConfig("akka.master-pinned-dispatcher"));
    }

    ///////////////////
    // Actor State   //
    ///////////////////

    private final ActorRef<Guardian.Message> guardian;
    private final ActorRef<DependencyMiner.Message> dependencyMiner;

    //////////////////////
    // Actor Behavior   //
    //////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(MiningFinishedMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage message) {
        this.dependencyMiner.tell(new DependencyMiner.StartMessage());
        return this;
    }

    private Behavior<Message> handle(MiningFinishedMessage message) {
        // Algorithm completed: trigger cluster shutdown via guardian.
        this.guardian.tell(new Guardian.ShutdownMessage(this.guardian));
        return this;
    }

    private Behavior<Message> handle(ShutdownMessage message) {
        // Optional: propagate shutdown to children for graceful stop.
        return Behaviors.stopped();
    }
}
