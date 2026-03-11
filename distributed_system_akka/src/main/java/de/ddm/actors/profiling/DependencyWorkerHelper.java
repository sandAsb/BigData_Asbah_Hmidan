package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import de.ddm.serialization.AkkaSerializable;
import it.unimi.dsi.fastutil.ints.*;
import lombok.*;

import java.util.List;

public class DependencyWorkerHelper extends AbstractBehavior<DependencyWorkerHelper.Message> {

    public interface Message extends AkkaSerializable {}

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class ValidateSliceTaskMessage implements Message {
        private static final long serialVersionUID = 1L;
        int taskId;
        IntList lhsSlice;
        IntList rhsCandidates;
    }

    public static final String DEFAULT_NAME = "dependencyWorkerHelper";

    public static Behavior<Message> create(
            Int2ObjectMap<List<String>> sortedDistinctColumns,
            ActorRef<DependencyWorkerCameo.Message> cameo
    ) {
        return Behaviors.setup(ctx -> new DependencyWorkerHelper(ctx, sortedDistinctColumns, cameo));
    }

    private final Int2ObjectMap<List<String>> sortedDistinctColumns;
    private final ActorRef<DependencyWorkerCameo.Message> cameo;

    private DependencyWorkerHelper(
            ActorContext<Message> context,
            Int2ObjectMap<List<String>> sortedDistinctColumns,
            ActorRef<DependencyWorkerCameo.Message> cameo
    ) {
        super(context);
        this.sortedDistinctColumns = sortedDistinctColumns;
        this.cameo = cameo;
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ValidateSliceTaskMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ValidateSliceTaskMessage msg) {
        IntArrayList validLHS = new IntArrayList();
        IntArrayList validRHS = new IntArrayList();

        IntList lhsSlice = msg.getLhsSlice();
        IntList rhsCandidates = msg.getRhsCandidates();

        for (int i = 0; i < lhsSlice.size(); i++) {
            int lhs = lhsSlice.getInt(i);
            List<String> left = sortedDistinctColumns.get(lhs);
            if (left == null) continue;

            for (int j = 0; j < rhsCandidates.size(); j++) {
                int rhs = rhsCandidates.getInt(j);
                if (lhs == rhs) continue;

                List<String> right = sortedDistinctColumns.get(rhs);
                if (right == null) continue;

                if (isSubset(left, right)) {
                    validLHS.add(lhs);
                    validRHS.add(rhs);
                }
            }
        }

        cameo.tell(new DependencyWorkerCameo.PartialResultMessage(msg.taskId, validLHS.toIntArray(), validRHS.toIntArray()));
        cameo.tell(new DependencyWorkerCameo.TaskDoneMessage(msg.taskId));

        return Behaviors.stopped();
    }

    private boolean isSubset(List<String> left, List<String> right) {
        int i = 0, j = 0;
        while (i < left.size() && j < right.size()) {
            int cmp = left.get(i).compareTo(right.get(j));
            if (cmp == 0) { i++; j++; }
            else if (cmp > 0) { j++; }
            else { return false; }
        }
        return i == left.size();
    }
}