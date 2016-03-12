import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * the hash partitioner
 * 
 * @author yingyib
 * 
 */
public class FirstPartitioner implements Partitioner<TextPair, Writable> {

  @Override
    public void configure(JobConf job) {
  }

  @Override
    public int getPartition(TextPair key, Writable value, int numPartitions) {
    return Math.abs(key.getFirst().hashCode()) % numPartitions;
  }
}