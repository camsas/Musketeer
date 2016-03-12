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

  public static class Map extends Mapper<Object, Text, NullWritable, Text> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
      String[] {{REL_NAME}} = value.toString().split(" ");
      if ({{CONDITION}}) {
        {{GET_COLUMNS}}
        context.write(NullWritable.get(), new Text({{COLUMNS}}.trim()));
      }
    }

  }

  public int run(String[] args) throws Exception{
    Configuration conf = new Configuration();
    Job job = new Job(conf, "{{CLASS_NAME}}");
    job.setJarByClass({{CLASS_NAME}}.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    FileInputFormat.addInputPath(job, new Path("{{INPUT_PATH}}"));
    FileOutputFormat.setOutputPath(job, new Path("{{OUTPUT_PATH}}"));
    return (job.waitForCompletion(true) ? 0 : 1);
  }
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new {{CLASS_NAME}}(), args);
    System.exit(res);
  }
}
