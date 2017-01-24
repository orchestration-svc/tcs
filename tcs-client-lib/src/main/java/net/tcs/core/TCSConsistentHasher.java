package net.tcs.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.task.coordinator.message.utils.TCSConstants;

public class TCSConsistentHasher<T> {

    public interface HashFunction {
        public int getHashValue(String key);
    }

    public static class TCSHashFunction implements HashFunction {
        private static final long FNV_BASIS = 0x811c9dc5;
        private static final long FNV_PRIME = (1 << 24) + 0x193;

        @Override
        public int getHashValue(String key) {
            final byte[] keyBytes = key.getBytes();
            long hash = FNV_BASIS;
            for (int i = 0; i < keyBytes.length; i++) {
                hash ^= 0xFF & keyBytes[i];
                hash *= FNV_PRIME;
            }
            return (int) hash;
        }
    }

    private final HashFunction hashFunction;
    private final int numberOfReplicas;
    private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();

    public TCSConsistentHasher(HashFunction hashFunction, int numberOfReplicas, Collection<T> nodes) {

        this.hashFunction = hashFunction;
        this.numberOfReplicas = numberOfReplicas;

        for (final T node : nodes) {
            add(node);
        }
    }

    private void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hashFunction.getHashValue(node.toString() + i), node);
        }
    }

    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hashFunction.getHashValue(node.toString() + i));
        }
    }

    public T get(Object k) {
        if (circle.isEmpty()) {
            return null;
        }
        final String key = (String) k;
        int hash = hashFunction.getHashValue(key);
        if (!circle.containsKey(hash)) {
            final SortedMap<Integer, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private static volatile TCSConsistentHasher<String> hasher;

    public static TCSConsistentHasher<String> getConsistentHasher(int numPartitions) {
        if (hasher == null) {
            synchronized (TCSConsistentHasher.class) {
                if (hasher == null) {
                    final List<String> vNodes = new ArrayList<>();
                    for (int i = 0; i < numPartitions; i++) {
                        vNodes.add(String.format("%s_%d", TCSConstants.TCS_SHARD_GROUP_NAME, i));
                    }
                    hasher = new TCSConsistentHasher<String>(new TCSHashFunction(), 1, vNodes);
                }
            }
        }
        return hasher;
    }
}
