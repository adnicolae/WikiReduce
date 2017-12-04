package part1.multi_job_version;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner class based on the HashPartitioner.
 */
public class Partition<K, V> extends Partitioner<K, V> {
    @Override
    public int getPartition(K k, V v, int i) {
        return (k.hashCode() & Integer.MAX_VALUE) % i;
    }
}
