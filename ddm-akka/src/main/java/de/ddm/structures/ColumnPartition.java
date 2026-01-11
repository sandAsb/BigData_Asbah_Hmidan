package de.ddm.structures;

import java.io.Serializable;
import java.util.Set;

public record ColumnPartition(
        ColumnPartitionId id,
        Set<String> values
) implements Serializable {
    private static boolean finalChunk = false;

    public void markFinal() { finalChunk = true; }

    public boolean isFinal() { return finalChunk; }
}
