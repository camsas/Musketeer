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

public class JoinMovRat extends Configured implements Tool {

  public static class Map extends Mapper<Object, Text, IntWritable, Text> {

    private boolean is_left_rel = false;

    public void setup(Context context)
        throws IOException, InterruptedException {
      String relation = ((FileSplit)context.getInputSplit()).getPath().getParent().getName();
      if (relation.compareTo("movies") == 0) {
        is_left_rel = true;
      }
    }

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String[] lineBuf = value.toString().trim().split(" ");
      if (this.is_left_rel) {
        // Select movies < 1970
        if (Integer.valueOf(lineBuf[1]) < 1970) {
          // Emit (mov_id).
          context.write(new IntWritable(Integer.valueOf(lineBuf[0])),
                        new Text("L "));
        }
      } else {
        // Emit (mov_id, (user_id, rating)).
        context.write(new IntWritable(Integer.valueOf(lineBuf[0])),
                      new Text("R " + lineBuf[1] + " " + lineBuf[2]));
      }
    }

  }

  public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text>{

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<String> right = new LinkedList<String>();
      boolean hasLeft = false;
      for (Text text : values) {
        String tmp = text.toString();
        if (tmp.charAt(0) == 'L') {
          hasLeft = true;
        } else {
          right.add(tmp.substring(2));
        }
      }
      if (hasLeft) {
        String movie_id = key.toString();
        for (String right_val : right) {
          context.write(NullWritable.get(),
                        new Text(movie_id + " " + right_val));
        }
      }
    }

  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "JoinMovRat");
    job.setJarByClass(JoinMovRat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(Integer.valueOf(args[3]));
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new JoinMovRat(), args);
    System.exit(res);
  }

}
