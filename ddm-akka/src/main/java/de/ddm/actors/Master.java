package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.*;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.serialization.AkkaSerializable;
import lombok.NoArgsConstructor;

public class Master extends AbstractBehavior<Master.Message> {

    public interface Message extends AkkaSerializable { }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -1963913294517850454L;
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 7516129288777469221L;
    }

    @NoArgsConstructor
    public static class MiningFinishedMessage implements Message {
        private static final long serialVersionUID = 1L;
    }

    public static final String DEFAULT_NAME = "master";

    public static Behavior<Message> create(ActorRef<Guardian.Message> guardian) {
        return Behaviors.setup(ctx -> new Master(ctx, guardian));
    }

    private final ActorRef<Guardian.Message> guardian;

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<DependencyMiner.Message> dependencyMiner;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    private Master(ActorContext<Message> context, ActorRef<Guardian.Message> guardian) {
        super(context);

        Reaper.watchWithDefaultReaper(context.getSelf());

        this.guardian = guardian;
        this.dependencyMiner = context.spawn(
                DependencyMiner.create(context.getSelf()),
                DependencyMiner.DEFAULT_NAME,
                DispatcherSelector.fromConfig("akka.master-pinned-dispatcher"));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMessage.class, this::handle)
                .onMessage(MiningFinishedMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(StartMessage msg) {
        dependencyMiner.tell(new DependencyMiner.StartMessage());
        return this;
    }

    private Behavior<Message> handle(MiningFinishedMessage msg) {
        guardian.tell(new Guardian.ShutdownMessage(guardian));
        return this;
    }

    private Behavior<Message> handle(ShutdownMessage msg) {
        dependencyMiner.tell(new DependencyMiner.ShutdownMessage());
        return this; // Reaper will stop us
    }
}
