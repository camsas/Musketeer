import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * group comparator
 * 
 * @author yingyib
 * 
 */
public class GroupComparator extends WritableComparator {
  protected GroupComparator() {
    super(TextPair.class, true);
  }

  @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    TextPair ip1 = (TextPair) w1;
    TextPair ip2 = (TextPair) w2;
    return ip1.getFirst().compareTo(ip2.getFirst());
  }
}