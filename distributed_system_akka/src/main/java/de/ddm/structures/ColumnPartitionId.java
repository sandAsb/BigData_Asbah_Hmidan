package de.ddm.structures;

import java.io.Serializable;

public record ColumnPartitionId(
        String table,
        String column,
        int shard
) implements Serializable {}
