package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FilteringWordCount extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringWordCountMapper.class,
        Text.class, IntWritable.class, WordCountReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
    wordCount.waitForCompletion(true);

    return 0;
  }

  static class FilteringWordCountMapper extends Mapper<Object,Text,Text,IntWritable> {
    
    // Use static instances to minimize allocations
	  private Text currentWord = new Text();
	  private final static IntWritable one = new IntWritable(1);
	  
	  private Set<String> stopwords = new HashSet<String>(
	      Arrays.asList(new String[] {"this", "the", "to", "and", "in", "or", "so"}));
	  
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      for (String word : line.toString().toLowerCase().split("[\\p{P} \\t\\n\\r]")) {
        if (! stopwords.contains(word)) {
          currentWord.set(word);
          ctx.write(currentWord, one);
        }
      }
    }
  }

  static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    private IntWritable res = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
        throws IOException, InterruptedException {
      int sum = 0;  // assuming that counts are not too big
      for (IntWritable i : values) {
        sum += i.get();
      }
      res.set(sum);
      ctx.write(key, res);
    }
  }

}