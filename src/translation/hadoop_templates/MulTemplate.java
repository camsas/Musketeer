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

    private int left_index = {{LEFT_COL_INDEX}};
    private int right_index = {{RIGHT_COL_INDEX}};

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
      String[] {{REL_NAME}} = value.toString().trim().split(" ");
      if ({{CONDITION}}) {
        int index = left_index == -1 ? right_index : left_index;
        String result = "";
        for (int i = 0; i < {{REL_NAME}}.length; i++) {
          if (i == index) {
            if (left_index == -1) {
              {{REL_NAME}}[i] = String.valueOf(
                {{RIGHT_TYPE}}.valueOf({{REL_NAME}}[right_index]) * {{VALUE}});
            } else {
              if (right_index == -1) {
                {{REL_NAME}}[i] = String.valueOf(
                  {{LEFT_TYPE}}.valueOf({{REL_NAME}}[left_index]) * {{VALUE}});
              } else {
                {{REL_NAME}}[i] = String.valueOf(
                  {{LEFT_TYPE}}.valueOf({{REL_NAME}}[left_index]) *
                  {{RIGHT_TYPE}}.valueOf({{REL_NAME}}[right_index]));
              }
            }
          }
          result += {{REL_NAME}}[i] + " ";
        }
        context.write(NullWritable.get(), new Text(result.trim()));
      }
    }
  }

  public int run(String[] args) throws Exception {
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
