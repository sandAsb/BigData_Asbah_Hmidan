package de.ddm.actors;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import de.ddm.actors.profiling.DataProvider;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * Master = top-level coordinator that boots the system:
 *  - spawns DataProviders (they register via Receptionist themselves)
 *  - spawns DependencyWorkers (they discover the miner via Receptionist and register)
 *  - spawns DependencyMiner and triggers StartMessage
 *
 * The miner will notify Master with MiningFinishedMessage.
 *
 * This Master is consistent with the "full codes" I provided in this chat:
 *  - DependencyMiner.create(ActorRef<Master.Message>)
 *  - Master.MiningFinishedMessage is sent by miner when done
 */
public class Master extends AbstractBehavior<Master.Message> {

    ////////////////////
    // Messages
    ////////////////////

    public interface Message extends AkkaSerializable {}

    /** Boot everything and start mining. */
    public static final class StartMiningMessage implements Message {
        private static final long serialVersionUID = 1L;
    }

    /** Sent by DependencyMiner when all validation tasks are completed. */
    public static final class MiningFinishedMessage implements Message {
        private static final long serialVersionUID = 1L;
    }

    /** Stop the whole system. */
    public static final class ShutdownMessage implements Message {
        private static final long serialVersionUID = 1L;
    }

    ////////////////////
    // Construction
    ////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Behavior<Message> create(ActorRef<Guardian.Message> self) {
        return Behaviors.setup(Master::new);
    }

    private Master(ActorContext<Message> context) {
        super(context);
    }

    ////////////////////
    // State
    ////////////////////

    private ActorRef<DependencyMiner.Message> miner;

    private final List<ActorRef<DataProvider.Message>> providers = new ArrayList<>();
    private final List<ActorRef<DependencyWorker.Message>> workers = new ArrayList<>();

    // Choose defaults that work locally; adjust if your project has config singletons.
    private final int numProviders = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
    private final int numWorkers = Math.max(2, Runtime.getRuntime().availableProcessors());

    private boolean started = false;

    ////////////////////
    // Behavior
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartMiningMessage.class, this::onStart)
                .onMessage(MiningFinishedMessage.class, this::onFinished)
                .onMessage(ShutdownMessage.class, this::onShutdown)
                .build();
    }

    private Behavior<Message> onStart(StartMiningMessage msg) {
        if (started) return this;
        started = true;

        // 1) Spawn providers first (they register via Receptionist in DataProvider.create()).
        for (int i = 0; i < numProviders; i++) {
            ActorRef<DataProvider.Message> p = getContext().spawn(
                    DataProvider.create(),
                    "dataProvider_" + i
            );
            providers.add(p);
        }

        // 2) Spawn workers (they will subscribe to miner service and register automatically).
        for (int i = 0; i < numWorkers; i++) {
            ActorRef<DataProvider.Message> provider = null;
            ActorRef<DependencyWorker.Message> w = getContext().spawn(
                    DependencyWorker.create(provider),
                    DependencyWorker.DEFAULT_NAME + "_" + i
            );
            workers.add(w);
        }

        // 3) Spawn miner and trigger it.
        miner = getContext().spawn(
                DependencyMiner.create(getContext().getSelf()),
                DependencyMiner.DEFAULT_NAME
        );

        miner.tell(new DependencyMiner.StartMessage());

        getContext().getLog().info(
                "Master started: {} providers, {} workers, miner={}",
                numProviders, numWorkers, miner.path().name()
        );

        return this;
    }

    private Behavior<Message> onFinished(MiningFinishedMessage msg) {
        getContext().getLog().info("Mining finished. Shutting down the actor system.");

        // Let ResultCollector finalize triggers Guardian shutdown in your code,
        // but we also safely terminate here.
        getContext().getSystem().terminate();

        return this;
    }

    private Behavior<Message> onShutdown(ShutdownMessage msg) {
        // Stop children (best-effort).
        if (miner != null) miner.tell(new DependencyMiner.ShutdownMessage());
        for (ActorRef<DependencyWorker.Message> w : workers) w.tell(new DependencyWorker.ShutdownMessage());

        // DataProvider has no ShutdownMessage in the code we used; system termination is enough.
        getContext().getSystem().terminate();
        return this;
    }

    public static class StartMessage implements Message {
    }
}