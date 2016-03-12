/*
 * Copyright 2009-2010 by The Regents of the University of California,
 * and University of Washington
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Naive implementation of PageRank on Hadoop
 * @author yingyib
 */
public class NaivePageRank {

  public static class CountMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, Text> {

    private Text one = new Text("1");
    private List<String> tokenList = new ArrayList<String>();
    private Text outputKey = new Text();

    public void map(LongWritable key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      tokenList.clear();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        tokenList.add(tokenizer.nextToken());
        if (tokenList.size() >= 2) {
          String keyStr = tokenList.get(0);
          outputKey.set(keyStr.getBytes());
          output.collect(outputKey, one);
        }
      }
    }
  }

  public static class CountReducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

    private Text count = new Text();

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += Integer.parseInt(values.next().toString());
      }
      count.set((Integer.toString(sum) + "#*").getBytes());
      output.collect(key, count);
    }
  }

 /**
  * the Mapper class for local rank computation
  * @author yingyib
  */
  public static class ComputeRankMap extends MapReduceBase implements
    Mapper<LongWritable, Text, TextPair, Text> {
    /* tag for rank_value tuple */
    private String tag0 = "0";
    /* tag for relation tuple */
    private String tag1 = "1";
    /* tag for relation tuple */
    private String tag2 = "2";
    private List<String> tokenList = new ArrayList<String>();
    private TextPair outputKey = new TextPair(new Text(), new Text());
    private Text outputValue = new Text();

    public void map(LongWritable key, Text value,
                    OutputCollector<TextPair, Text> output, Reporter reporter)
        throws IOException {
      tokenList.clear();
      String line = value.toString();
      if (line.startsWith("#"))
        return;

      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens())
        tokenList.add(tokenizer.nextToken().trim());

      if (tokenList.size() >= 2) {
        String valueToken = tokenList.get(1);
        if ((valueToken.indexOf(".") >= 0 && valueToken.split("\\.").length <= 2)) {
          outputKey.setSecondText(tag0);
          outputValue.set(valueToken.getBytes());
        } else if (valueToken.endsWith("#*")) {
          String valueCount = valueToken.substring(0, valueToken.length() - 2);
          outputKey.setSecondText(tag1);
          outputValue.set(valueCount.getBytes());
        } else {
          outputKey.setSecondText(tag2);
          outputValue.set(valueToken.getBytes());
        }
        outputKey.setFirstText(tokenList.get(0));
        output.collect(outputKey, outputValue);
      }
    }
  }

  public static class ComputeRankReduce extends MapReduceBase implements
    Reducer<TextPair, Text, Text, Text> {

    private float srcRank = 0;
    private Text value = new Text();

    public void configure(JobConf conf) {
      value.clear();
      byte buffer[] = new byte[100];
      value.append(buffer, 0, buffer.length);
    }

    public void reduce(TextPair key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if (!values.hasNext())
        return;
      String rankValue = values.next().toString();
      srcRank = Float.parseFloat(rankValue);
      float selfRank = srcRank;
      if (!values.hasNext())
        return;
      String countStr = values.next().toString();
      float count = Float.parseFloat(countStr);
      if (count == 0)
        return;
      srcRank = srcRank / count;
      while (values.hasNext()) {
        Text dest = values.next();
        value.clear();
        String rank = Float.toString(srcRank);
        byte[] rankBytes = rank.getBytes();
        value.append(rankBytes, 0, rankBytes.length);
        output.collect(dest, value);
      }
      value.clear();
      String rank = Float.toString(selfRank);
      byte[] rankBytes = rank.getBytes();
      value.append(rankBytes, 0, rankBytes.length);
      output.collect(key.getFirst(), value);
    }
  }

  public static class InitialRankAssignmentMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {

    private Text startValue = new Text("1");
    private Text outputKey = new Text();
    private List<String> tokenList = new ArrayList<String>();

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
      tokenList.clear();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        tokenList.add(tokenizer.nextToken());
      }
      if (tokenList.size() >= 2) {
        outputKey.set(tokenList.get(0).getBytes());
        output.collect(outputKey, startValue);
        outputKey.set(tokenList.get(1).getBytes());
        output.collect(outputKey, startValue);
      }
    }
  }

  public static class InitialRankAssignmentReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {

    Text initValue = new Text("1.0");

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
      output.collect(key, initValue);
    }
  }

  public static class RankAggregateMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private List<String> tokenList = new ArrayList<String>();

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      tokenList.clear();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        tokenList.add(tokenizer.nextToken());
      }
      if (tokenList.size() >= 2) {
        outputKey.set(tokenList.get(0).getBytes());
        outputValue.set(tokenList.get(1).getBytes());
        output.collect(outputKey, outputValue);
      }
    }
  }

  public static class RankAggregateReducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();
    private static final float dampingFactor = 0.15f;
    private int numNodes = 0;
    private float prefix = 0f;

    public void configure(JobConf conf) {
      // default number from the number of nodes in soc-LiveJournal1 data
      this.numNodes = conf.getInt("haloop.num.nodes", 4847571);
      this.prefix = (1.0f - dampingFactor) / numNodes;
    }

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      float totalRank = 0;
      while (values.hasNext()) {
        totalRank += Float.valueOf(values.next().toString());
      }
      totalRank = prefix + dampingFactor * totalRank;
      // output total rank
      outputValue.set(Float.toString(totalRank).getBytes());
      output.collect(key, outputValue);
    }
  }

  public static void main(String[] args) throws Exception {
    int iteration = -1;
    String inputPath = args[0];
    String outputPath = args[1];
    int specIteration = 0;
    if (args.length > 2) {
      specIteration = Integer.parseInt(args[2]);
    }
    int numNodes = 100000;
    if (args.length > 3) {
      numNodes = Integer.parseInt(args[3]);
    }
    int numReducers = 32;
    if (args.length > 4) {
      numReducers = Integer.parseInt(args[4]);
    }
    System.out.println("specified iteration: " + specIteration);
    long start = System.currentTimeMillis();

    /**
     * job to count out-going links for each url
     */
    JobConf conf = new JobConf(NaivePageRank.class);
    conf.setJobName("PageRank-Count");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(CountMapper.class);
    conf.setReducerClass(CountReducer.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/count"));
    conf.setNumReduceTasks(numReducers);
    JobClient.runJob(conf);

    /******************** Initial Rank Assignment Job ***********************/
    conf = new JobConf(NaivePageRank.class);
    conf.setJobName("PageRank-Initialize");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(InitialRankAssignmentMapper.class);
    conf.setReducerClass(InitialRankAssignmentReducer.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf,
                                   new Path(outputPath + "/i" + iteration));
    conf.setNumReduceTasks(numReducers);
    // conf.setIterative(false);
    JobClient.runJob(conf);
    iteration++;

    do {
      /****************** Join Job ********************************/
      conf = new JobConf(NaivePageRank.class);
      conf.setJobName("PageRank-Join");
      conf.setOutputKeyClass(Text.class);
      // conf.setOutputValueClass(Text.class);
      conf.setMapperClass(ComputeRankMap.class);
      conf.setReducerClass(ComputeRankReduce.class);
      conf.setMapOutputKeyClass(TextPair.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      conf.setPartitionerClass(FirstPartitioner.class);
      conf.setOutputKeyComparatorClass(KeyComparator.class);
      conf.setOutputValueGroupingComparator(GroupComparator.class);

      // relation table
      FileInputFormat.setInputPaths(conf, new Path(inputPath));
      // rank table
      FileInputFormat.addInputPath(conf,
                                   new Path(outputPath + "/i" + (iteration - 1)));
      // count table
      FileInputFormat.addInputPath(conf, new Path(outputPath + "/count"));
      FileOutputFormat.setOutputPath(conf,
                                     new Path(outputPath + "/i" + iteration));
      conf.setNumReduceTasks(numReducers);
      JobClient.runJob(conf);
      iteration++;

      /******************** Rank Aggregate Job ***********************/
      conf = new JobConf(NaivePageRank.class);
      conf.setJobName("PageRank-Aggregate");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
      conf.setMapOutputKeyClass(Text.class);
      conf.setMapperClass(RankAggregateMapper.class);
      conf.setReducerClass(RankAggregateReducer.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      FileInputFormat.setInputPaths(conf,
                                    new Path(outputPath + "/i" + (iteration - 1)));
      FileOutputFormat.setOutputPath(conf,
                                     new Path(outputPath + "/i" + iteration));
      conf.setNumReduceTasks(numReducers);
      conf.setInt("haloop.num.nodes", numNodes);
      JobClient.runJob(conf);
      iteration++;
    } while (iteration < 2 * specIteration);

    long end = System.currentTimeMillis();
    System.out.println("running time " + (end - start) / 1000 + "s");
  }

}
