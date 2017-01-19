/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import io.bespin.java.util.PairOfFloatInt;
import io.bespin.java.util.PairOfStrings;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.net.URI;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.lang.Integer;
import java.lang.Math;

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  
  // MapReduce Job1 is used to accumulate line counts of individual words as well at the total line count.
  private static final class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Text WORD = new Text();
    private static final IntWritable ONE = new IntWritable(1);
    private Map<String, Integer> counts;

    @Override
    public void setup(Context context) {
      counts = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      List<String> tokens = Tokenizer.tokenize(value.toString());

      for (int i = 0; i < Math.min(40, tokens.size()); i++) {
        String word = tokens.get(i);
        if (!counts.containsKey(word)) {
          WORD.set(word);
          context.write(WORD, ONE);
          counts.put(word, 1);
        }
      }
      counts.clear();
      WORD.set("*");
      context.write(WORD, ONE);
    }
  }

  private static final class MyReducer1 extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    private Map<String, Integer> counts;

    @Override
    public void setup(Context context) {
      counts = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      for (int i = 0; i < Math.min(40, tokens.size()); i++) {
        for (int j = 0; j < Math.min(40, tokens.size()); j++) {
          if (i == j) continue;
          String pair = tokens.get(i) + " " + tokens.get(j);
          if (!counts.containsKey(pair)) {
            PAIR.set(tokens.get(i), tokens.get(j));
            context.write(PAIR, ONE);
            counts.put(pair, 1);
          }
        }
      }
      counts.clear();
    }
  }

  private static final class MyCombiner2 extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class MyReducer2 extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {
    private static final PairOfFloatInt PAIR = new PairOfFloatInt();
    private Map<String, Integer> counts;

    @Override
    public void setup(Context context) throws IOException {
      counts = new HashMap<>();
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
      for (int i = 0; i < cacheFiles.length; ++i) {
        Path path = new Path(cacheFiles[i].getPath());  
        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
        String line = null;
        while ((line = bf.readLine()) != null) {
          String[] pair = line.split("\\s+");
          counts.put(pair[0], Integer.parseInt(pair[1]));
        }
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int threshold = Integer.parseInt(context.getConfiguration().get("threshold"));
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      if (sum >= threshold) {
        float arg = (float)counts.get("*") * sum / (counts.get(key.getLeftElement())*counts.get(key.getRightElement()));
        PAIR.set((float)Math.log10(arg), sum);
        context.write(key, PAIR);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
    int window = 2;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurance")
    int threshold = 0;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - window: " + args.window);
    LOG.info(" - number of reducers: " + args.numReducers);

    // First Job:
    Job job1 = Job.getInstance(getConf());
    job1.setJobName(PairsPMI.class.getSimpleName());
    job1.setJarByClass(PairsPMI.class);
    String temp = "intermediateData";
    // Delete the output directory if it exists already.
    Path outputDir = new Path(temp);
    FileSystem.get(getConf()).delete(outputDir, true);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(temp));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    job1.setMapperClass(MyMapper1.class);
    job1.setCombinerClass(MyReducer1.class);
    job1.setReducerClass(MyReducer1.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    // Second Job:
    Configuration conf = getConf();
    conf.set("threshold", Integer.toString(args.threshold));
    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    // Cache intermediate data:
    FileSystem fs = FileSystem.get(new Configuration());
    Path cacheFile = new Path(temp);
    FileStatus[] list = fs.listStatus(cacheFile);
    for (FileStatus status : list) {
      DistributedCache.addCacheFile(status.getPath().toUri(), job2.getConfiguration());
    }

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloatInt.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setCombinerClass(MyCombiner2.class);
    job2.setReducerClass(MyReducer2.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
