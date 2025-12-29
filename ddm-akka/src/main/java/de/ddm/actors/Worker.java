package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.*;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class Worker extends AbstractBehavior<Worker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////
	public interface Message extends AkkaSerializable { }

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";

	public static Behavior<Message> create() {
		return Behaviors.setup(Worker::new);
	}

	private Worker(ActorContext<Message> context) {
		super(context);

		Reaper.watchWithDefaultReaper(context.getSelf());

		int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();
		this.workers = new ArrayList<>(numWorkers);

		for (int i = 0; i < numWorkers; i++)
			workers.add(context.spawn(
					DependencyWorker.create(),
					DependencyWorker.DEFAULT_NAME + "_" + i,
					DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")));
	}

	///////////////////
	// Actor State //
	///////////////////

	private final List<ActorRef<DependencyWorker.Message>> workers; // more than one

	//////////////////////
	// Actor Behavior //
	//////////////////////
	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ShutdownMessage msg) {
		for (ActorRef<DependencyWorker.Message> w : workers)
			w.tell(new DependencyWorker.ShutdownMessage());

		return this; // Reaper handles termination
	}
}
