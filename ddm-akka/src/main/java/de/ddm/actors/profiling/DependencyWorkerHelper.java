package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import de.ddm.serialization.AkkaSerializable;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

public class DependencyWorkerHelper extends AbstractBehavior<DependencyWorkerHelper.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable { }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidateLocalTaskMessage implements Message {
        private static final long serialVersionUID = 1L;
        IntList attributes1;
        IntList attributes2;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidateTaskMessage implements Message {
        private static final long serialVersionUID = 1L;
        int attribute;
        List<String> column;
        IntList localAttributes;
    }

    //////////////////////////
    // Actor Construction //
    //////////////////////////

    public static final String DEFAULT_NAME = "dependencyWorkerHelper";

    public static Behavior<Message> create(
            Int2ObjectMap<List<String>> sortedColumns,
            ActorRef<DependencyWorkerCameo.Message> cameo
    ) {
        return Behaviors.setup(ctx -> new DependencyWorkerHelper(ctx, sortedColumns, cameo));
    }

    private DependencyWorkerHelper(
            ActorContext<Message> context,
            Int2ObjectMap<List<String>> sortedColumns,
            ActorRef<DependencyWorkerCameo.Message> cameo
    ) {
        super(context);
        this.sortedColumns = sortedColumns;
        this.cameo = cameo;
    }

    ///////////////////
    // Actor State  //
    ///////////////////

    private final Int2ObjectMap<List<String>> sortedColumns;
    private final ActorRef<DependencyWorkerCameo.Message> cameo;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ValidateLocalTaskMessage.class, this::handle)
                .onMessage(ValidateTaskMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ValidateLocalTaskMessage msg) {
        IntArrayList validLHS = new IntArrayList();
        IntArrayList validRHS = new IntArrayList();

        for (int i = 0; i < msg.getAttributes1().size(); i++) {
            int a = msg.getAttributes1().getInt(i);
            int b = msg.getAttributes2().getInt(i);

            if (isSubset(sortedColumns.get(a), sortedColumns.get(b))) {
                validLHS.add(a);
                validRHS.add(b);
            }
        }

        cameo.tell(new DependencyWorkerCameo.PartialResultMessage(
                validLHS.toIntArray(),
                validRHS.toIntArray()
        ));
        return this;
    }

    private Behavior<Message> handle(ValidateTaskMessage msg) {
        IntArrayList validLHS = new IntArrayList();
        IntArrayList validRHS = new IntArrayList();

        for (int localAttr : msg.getLocalAttributes()) {
            if (isSubset(msg.getColumn(), sortedColumns.get(localAttr))) {
                validLHS.add(msg.getAttribute());
                validRHS.add(localAttr);
            }
        }

        cameo.tell(new DependencyWorkerCameo.PartialResultMessage(
                validLHS.toIntArray(),
                validRHS.toIntArray()
        ));
        return this;
    }

    private boolean isSubset(List<String> left, List<String> right) {
        int i = 0, j = 0;
        while (i < left.size() && j < right.size()) {
            int cmp = left.get(i).compareTo(right.get(j));
            if (cmp == 0) {
                i++; j++;
            } else if (cmp > 0) {
                j++;
            } else {
                return false;
            }
        }
        return i == left.size();
    }
}
