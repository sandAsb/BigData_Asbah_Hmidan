package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.profiling.DataProvider;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;

import java.util.ArrayList;
import java.util.List;

public class Worker extends AbstractBehavior<Worker.Message> {

    public static final String DEFAULT_NAME = "worker";
    public interface Message extends AkkaSerializable {}

    public static final class ShutdownMessage implements Message {}

    public static Behavior<Message> create() {
        return Behaviors.setup(Worker::new);
    }

    private final ActorRef<DataProvider.Message> provider;
    private final List<ActorRef<DependencyWorker.Message>> workers;

    private Worker(ActorContext<Message> ctx) {
        super(ctx);

        provider = ctx.spawn(DataProvider.create(), "dataProvider");

        int n = Math.max(1, SystemConfigurationSingleton.get().getNumWorkers());
        workers = new ArrayList<>(n);

        for (int i = 0; i < n; i++) {
            workers.add(ctx.spawn(
                    DependencyWorker.create(provider),
                    "dependencyWorker-" + i
            ));
        }
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ShutdownMessage.class, msg -> {
                    workers.forEach(w -> w.tell(new DependencyWorker.ShutdownMessage()));
                    return Behaviors.stopped();
                })
                .build();
    }
}
