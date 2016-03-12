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

  public static class Map extends Mapper<Object, Text, Text, Text> {

    private {{COL_TYPE}} maxValue;

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
      String[] {{REL_NAME}} = value.toString().trim().split(" ");
      if ({{CONDITION}}) {
        {{COL_TYPE}} col_value = {{COL_TYPE}}.valueOf({{REL_NAME}}[{{COL_INDEX}}]);
        if ({{GROUP_BY}} == -1) {
          if (maxValue.compareTo(col_value) < 0 || maxValue == null) {
            maxValue = col_value;
          }
        } else {
          Text newKey = new Text({{GROUP_BY_KEY}});
          context.write(newKey, new Text(String.valueOf(col_value)));
        }
      }
    }

    @Override
    public void cleanup(Context context)
      throws IOException, InterruptedException {
      if ({{GROUP_BY}} == -1) {
        context.write(new Text("ALL"), new Text(String.valueOf(maxValue)));
      }
    }

  }

  public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      {{MAX_VALUE}}
      for (Text text : values) {
        {{COL_TYPE}} value = {{COL_TYPE}}.valueOf(text.toString());
        if (maxValue.compareTo(value) < 0) {
          maxValue = value;
        }
      }
      context.write(NullWritable.get(),
        new Text(key.toString() + " " + {{COL_TYPE}}.toString(maxValue)));
    }

  }

  public int run(String[] args) throws Exception{
    Configuration conf = new Configuration();
    Job job = new Job(conf, "{{CLASS_NAME}}");
    job.setJarByClass({{CLASS_NAME}}.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
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
