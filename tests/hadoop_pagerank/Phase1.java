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

public class Phase1 extends Configured implements Tool {

  public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] lineBuf = value.toString().trim().split(" ");
      context.write(new IntWritable(Integer.valueOf(lineBuf[0])),
                    new IntWritable(Integer.valueOf(lineBuf[1])));
    }

  }

  public static class Reduce extends Reducer<IntWritable, IntWritable, NullWritable, Text>{

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
      List<Integer> dsts = new LinkedList<Integer>();
      for (IntWritable value : values) {
        dsts.add(value.get());
      }
      int cnt = dsts.size();
      for (Integer dst : dsts) {
        context.write(NullWritable.get(),
                      new Text(key.get() + " " + dst + " " + cnt));
      }
    }

  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Phase1");
    job.setJarByClass(Phase1.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(16);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Phase1(), args);
    System.exit(res);
  }

}
