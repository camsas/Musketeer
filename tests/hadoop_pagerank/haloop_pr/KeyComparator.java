import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * key comparator
 * 
 * @author yingyib
 */
public class KeyComparator extends WritableComparator {
  
  protected KeyComparator() {
    super(TextPair.class, true);
  }

  @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    TextPair ip1 = (TextPair) w1;
    TextPair ip2 = (TextPair) w2;
    int cmp = ip1.getFirst().compareTo(ip2.getFirst());
    if (cmp != 0) {
      return cmp;
    }
    return ip1.getSecond().compareTo(ip2.getSecond()); 
  }
}