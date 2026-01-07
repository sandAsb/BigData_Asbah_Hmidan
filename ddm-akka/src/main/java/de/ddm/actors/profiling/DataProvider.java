package de.ddm.actors.profiling;

import akka.actor.typed.*;
import akka.actor.typed.javadsl.*;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.utils.MemoryUtils;

import java.util.HashMap;
import java.util.Map;

public class DataProvider extends AbstractBehavior<DataProvider.Message> {
    public interface Message extends AkkaSerializable {}
    public static class StorePartition implements Message {
        public final int attribute;
        public final String[] column;

        public StorePartition(int attribute, String[] column) {
            this.attribute = attribute;
            this.column = column;
        }
    }
    public static class RequestPartition implements Message {
        public final int attribute;
        public final ActorRef<ResponsePartition> replyTo;

        public RequestPartition(int attribute, ActorRef<ResponsePartition> replyTo) {
            this.attribute = attribute;
            this.replyTo = replyTo;
        }
    }
    public static class ResponsePartition implements Message {
        public final int attribute;
        public final String[] column;

        public ResponsePartition(int attribute, String[] column) {
            this.attribute = attribute;
            this.column = column;
        }
    }
    public static Behavior<Message> create() {
        return Behaviors.setup(DataProvider::new);
    }

    private DataProvider(ActorContext<Message> context) {
        super(context);
        this.maxWorkingMemory =
                MemoryUtils.bytesMax() - MemoryUtils.bytesUsed();
    }
    private final Map<Integer, String[]> partitions = new HashMap<>();

    private final long maxWorkingMemory;
    private long usedMemory = 0L;
    private static final double MEMORY_LIMIT = 0.7;
    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(StorePartition.class, this::handle)
                .onMessage(RequestPartition.class, this::handle)
                .build();
    }
    private Behavior<Message> handle(StorePartition msg) {
        long size = MemoryUtils.byteSizeOf(msg.column);

        if (usedMemory + size > MEMORY_LIMIT * maxWorkingMemory) {
            return this;
        }

        partitions.put(msg.attribute, msg.column);
        usedMemory += size;

        return this;
    }

    private Behavior<Message> handle(RequestPartition msg) {
        String[] column = partitions.get(msg.attribute);

        if (column != null) {
            msg.replyTo.tell(new ResponsePartition(msg.attribute, column));
        }

        return this;
    }
}
