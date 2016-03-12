import java.io.IOException;
import java.util.*;
import java.text.*;

public class Udf {

  public String evaluate(String line) {
    String[] lineBuf = line.split(" ");
    int sum = 0;
    for (int i = 0; i < lineBuf.length; ++i) {
      sum += Integer.valueOf(lineBuf[i]);
    }
    return String.valueOf(sum);
  }

}