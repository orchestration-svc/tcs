package net.tcs.core;

public interface ShardAllocator {

    public String getShardOwnerId();

    public int getTotalPartitionCount();
}
