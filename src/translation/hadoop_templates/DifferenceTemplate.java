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

public class {{CLASS_NAME}} extends Configured implements Tool {

  public static class Map extends Mapper<Object, Text, Text, BooleanWritable> {

    private boolean is_left_rel = false;

    public void setup(Context context)
        throws IOException, InterruptedException {
      String relation = ((FileSplit)context.getInputSplit()).getPath()
        .getParent().getName();
      if (relation.compareTo("{{LEFT_REL}}") == 0) {
        is_left_rel = true;
      }
    }

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      context.write(value, new BooleanWritable(is_left_rel));
    }

  }

  public static class Reduce extends
    Reducer<Text, BooleanWritable, NullWritable, Text> {

    public void reduce(Text key, Iterable<BooleanWritable> values,
                       Context context)
      throws IOException, InterruptedException {
      BooleanWritable true_bool = new BooleanWritable(true);
      int cnt = 0;
      boolean right_rel = false;
      for (BooleanWritable value : values) {
        if (value.equals(true_bool)) {
          cnt++;
        } else {
          right_rel = true;
        }
      }
      if (!right_rel) {
        for (; cnt > 0; cnt--) {
          context.write(NullWritable.get(), key);
        }
      }
    }

  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "{{CLASS_NAME}}");
    job.setJarByClass({{CLASS_NAME}}.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BooleanWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    FileInputFormat.addInputPath(job, new Path("{{LEFT_PATH}}"));
    FileInputFormat.addInputPath(job, new Path("{{RIGHT_PATH}}"));
    FileOutputFormat.setOutputPath(job, new Path("{{OUTPUT_PATH}}"));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new {{CLASS_NAME}}(), args);
    System.exit(res);
  }

}
