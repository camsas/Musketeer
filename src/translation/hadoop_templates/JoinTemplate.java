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

    private boolean is_left_rel = false;

    public void setup(Context context)
        throws IOException, InterruptedException {
      String relation = ((FileSplit)context.getInputSplit()).getPath().getParent().getName();
      if (relation.compareTo("{{LEFT_REL}}") == 0) {
        is_left_rel = true;
      }
    }

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] lineBuf = value.toString().trim().split(" ");
      if (this.is_left_rel) {
        context.write(new Text(lineBuf[{{LEFT_INDEX}}]),
                      new Text("L " + value.toString()));
      } else {
        context.write(new Text(lineBuf[{{RIGHT_INDEX}}]),
                      new Text("R " + value.toString()));
      }
    }

  }

  public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      ArrayList<String> arrayLeft = new ArrayList<String>();
      ArrayList<String> arrayRight = new ArrayList<String>();
      for (Text text : values) {
        String tmp = text.toString();
        if (tmp.charAt(0) == 'L') {
          arrayLeft.add(tmp.substring(2));
        } else {
          arrayRight.add(tmp.substring(2));
        }
      }
      for (int i = 0; i < arrayLeft.size(); i++) {
        for (int j = 0; j < arrayRight.size(); j++) {
          String[] rightBuf = arrayRight.get(j).split(" ");
          String right_rel = "";
          for (int k = 0; k < rightBuf.length; k++) {
            if (k != {{RIGHT_INDEX}}) {
              right_rel += " " + rightBuf[k];
            }
          }
          context.write(NullWritable.get(),
                        new Text(arrayLeft.get(i) + right_rel));
        }
      }
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "{{CLASS_NAME}}");
    job.setJarByClass({{CLASS_NAME}}.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
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
