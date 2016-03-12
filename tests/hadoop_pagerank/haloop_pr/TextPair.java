import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable {

  /**
   * the first field
   */
  Text first;

  /**
   * the second field
   */
  Text second;

  public TextPair() {

  }

  public TextPair(Text t1, Text t2) {
    first = t1;
    second = t2;
  }
  
  /**
   * set the first Text
   * @param t1
   */
  public void setFirstText(String t1) {
    first.set(t1.getBytes());
  }
  
  /**
   * set the second text
   * @param t2
   */
  public void setSecondText(String t2) {
    second.set(t2.getBytes());
  }

  /**
   * get the first field
   * 
   * @return the first field
   */
  public Text getFirst() {
    return first;
  }

  /**
   * get the second field
   * 
   * @return the second field
   */
  public Text getSecond() {
    return second;
  }

  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    if (first == null)
      first = new Text();

    if (second == null)
      second = new Text();

    first.readFields(in);
    second.readFields(in);
  }

  public int compareTo(Object object) {
    TextPair ip2 = (TextPair) object;
    int cmp = getFirst().compareTo(ip2.getFirst());
    if (cmp != 0) {
      return cmp;
    }
    return getSecond().compareTo(ip2.getSecond()); // reverse
  }

  public int hashCode() {
    return first.hashCode();
  }

  public boolean equals(Object o) {
    TextPair p = (TextPair) o;
    return first.equals(p.getFirst());
  }
}