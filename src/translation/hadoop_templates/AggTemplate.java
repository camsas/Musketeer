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

    {{TYPE}} aggValue;

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
      String[] {{REL_NAME}} = value.toString().trim().split(" ");
      if ({{CONDITION}}) {
        if ({{GROUP_BY}} == -1) {
          {{TYPE}} col_val = {{TYPE}}.valueOf({{REL_NAME}}[{{COL_INDEX}}]);
          if (aggValue == null) {
            aggValue = col_val;
          } else {
            aggValue = aggValue {{OPERATOR}} col_val;
          }
        } else {
          Text newKey = new Text({{GROUP_BY_KEY}});
          context.write(newKey, new Text({{REL_NAME}}[{{COL_INDEX}}]));
        }
      }
    }

    @Override
    public void cleanup(Context context)
      throws IOException, InterruptedException {
      if ({{GROUP_BY}} == -1) {
        context.write(new Text("ALL"), new Text(String.valueOf(aggValue)));
      }
    }

  }

  public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      int agg = {{INIT_VAL}};
      for (Text text : values) {
        agg {{OPERATOR}}= {{TYPE}}.valueOf(text.toString());
      }
      context.write(NullWritable.get(),
                    new Text(key.toString() + " " + String.valueOf(agg)));
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