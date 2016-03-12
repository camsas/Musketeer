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

  public static class Map extends Mapper<Object, Text, {{MAP_KEY_TYPE}}, {{MAP_VALUE_TYPE}}> {

{{MAP_VARIABLES_CODE}}
    private int index;

    public void setup(Context context)
        throws IOException, InterruptedException {
{{SETUP_CODE}}
    }


    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      {{INPUT_CODE}}
{{MAP_CODE}}
    }

    @Override
    public void cleanup(Context context)
      throws IOException, InterruptedException {
{{CLEANUP_CODE}}
    }

  }

  public static class Reduce extends Reducer<{{MAP_KEY_TYPE}}, {{MAP_VALUE_TYPE}}, {{REDUCE_KEY_TYPE}}, {{REDUCE_VALUE_TYPE}}>{

    private int index;

    public void reduce({{MAP_KEY_TYPE}} key, Iterable<{{MAP_VALUE_TYPE}}> values, Context context)
      throws IOException, InterruptedException {
{{REDUCE_CODE}}
    }

  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "{{CLASS_NAME}}");
    job.setJarByClass({{CLASS_NAME}}.class);
    job.setMapOutputKeyClass({{MAP_KEY_TYPE}}.class);
    job.setMapOutputValueClass({{MAP_VALUE_TYPE}}.class);
    job.setOutputKeyClass({{REDUCE_KEY_TYPE}}.class);
    job.setOutputValueClass({{REDUCE_VALUE_TYPE}}.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(16);
    {{INPUT_PATHS}}
    FileOutputFormat.setOutputPath(job, new Path("{{OUTPUT_PATH}}"));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new {{CLASS_NAME}}(), args);
    System.exit(res);
  }

}
