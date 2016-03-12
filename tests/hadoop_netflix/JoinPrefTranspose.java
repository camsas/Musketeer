import java.io.IOException;
import java.util.*;
import java.text.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.*;

public class JoinPrefTranspose extends Configured implements Tool {

  public static class Map extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] lineBuf = value.toString().trim().split(" ");
      // lineBuf[0] = pref[0], lineBuf[1] = pref[1], lineBuf[2] = pref[2]
      // trans[0] = lineBuf[1], trans[1] = lineBuf[0], lineBuf[2] = pref[2]
      context.write(new Text(lineBuf[1]), value);
    }

  }

  public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      List<String> arrayLeft = new LinkedList<String>();
      List<String> arrayRight = new LinkedList<String>();
      for (Text text : values) {
        String[] lineBuf = text.toString().trim().split(" ");
        // p1 p3
        arrayLeft.add(lineBuf[0] + " " + lineBuf[2]);
        // t1 t2
        arrayRight.add(lineBuf[0] + " " + lineBuf[2]);
      }
      for (String leftVal : arrayLeft) {
        String[] leftBuf = leftVal.split(" ");
        for (String rightVal : arrayRight) {
          String[] rightBuf = rightVal.split(" ");
          Integer mulVal = Integer.valueOf(leftBuf[1]) *
            Integer.valueOf(rightBuf[1]);
          context.write(NullWritable.get(),
                        new Text(leftBuf[0] + " " + mulVal + " " +
                                 rightBuf[0]));
        }
      }
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "JoinPrefTranspose");
    job.setJarByClass(JoinPrefTranspose.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(Integer.valueOf(args[2]));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new JoinPrefTranspose(),
                             args);
    System.exit(res);
  }

}
