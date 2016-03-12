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

  public static class Map extends Mapper<Object, Text, Text, IntWritable> {

    private int cnt = 0;

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
      String[] {{REL_NAME}} = value.toString().trim().split(" ");
      if ({{CONDITION}}) {
        if ({{GROUP_BY}} == -1) {
          cnt += 1;
        } else {
          Text newKey = new Text({{GROUP_BY_KEY}});
          context.write(newKey, new IntWritable(1));
        }
      }
    }

    @Override
    public void cleanup(Context context)
      throws IOException, InterruptedException {
      if ({{GROUP_BY}} == -1) {
        context.write(new Text("ALL"), new IntWritable(cnt));
      }
    }

  }

  public static class Reduce extends Reducer<Text, IntWritable, NullWritable, Text>{

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int cnt = 0;
      for (IntWritable value : values) {
        cnt += value.get();
      }
      context.write(NullWritable.get(),
                    new Text(key.toString() + " " + String.valueOf(cnt)));
    }

  }

  public int run(String[] args) throws Exception{
    Configuration conf = new Configuration();
    Job job = new Job(conf, "{{CLASS_NAME}}");
    job.setJarByClass({{CLASS_NAME}}.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    FileInputFormat.addInputPath(job, new Path("{{INPUT_PATH}}"));
    FileOutputFormat.setOutputPath(job, new Path("{{OUTPUT_PATH}}"));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new {{CLASS_NAME}}(), args);
    System.exit(res);
  }

}
