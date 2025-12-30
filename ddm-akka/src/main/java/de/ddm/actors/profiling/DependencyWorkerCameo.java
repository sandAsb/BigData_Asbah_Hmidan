package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class DependencyWorkerCameo extends AbstractBehavior<DependencyWorkerCameo.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable { }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PartialResultMessage implements Message {
        private static final long serialVersionUID = 1L;
        int[] lhss;
        int[] rhss;
    }

    //////////////////////////
    // Actor Construction //
    //////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorkerCameo";

    public static Behavior<Message> create(
            ActorRef<DependencyMiner.Message> miner,
            int numHelpers,
            int workerId
    ) {
        return Behaviors.setup(ctx -> new DependencyWorkerCameo(ctx, miner, numHelpers, workerId));
    }

    private DependencyWorkerCameo(
            ActorContext<Message> context,
            ActorRef<DependencyMiner.Message> miner,
            int numHelpers,
            int workerId
    ) {
        super(context);
        this.miner = miner;
        this.remaining = numHelpers;
        this.workerId = workerId;
    }

    ///////////////////
    // Actor State  //
    ///////////////////

    private final ActorRef<DependencyMiner.Message> miner;
    private final int workerId;
    private int remaining;

    private final List<Integer> lhss = new ArrayList<>();
    private final List<Integer> rhss = new ArrayList<>();

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(PartialResultMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(PartialResultMessage msg) {
        for (int i = 0; i < msg.getLhss().length; i++) {
            lhss.add(msg.getLhss()[i]);
            rhss.add(msg.getRhss()[i]);
        }

        remaining--;

        if (remaining == 0) {
            miner.tell(new DependencyMiner.ResultMessage(
                    lhss.stream().mapToInt(i -> i).toArray(),
                    rhss.stream().mapToInt(i -> i).toArray(),
                    workerId
            ));
            return Behaviors.stopped();
        }

        return this;
    }
}
