package de.tuberlin.dima.aim3.assignment1;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.tuberlin.dima.aim3.HadoopJob;

public class AverageTemperaturePerMonth extends HadoopJob {
  
  private static final String QUALITY_PARAM_NAME = "minimum-quality";

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, 
        AverageTemparatureMapper.class, Text.class, IntWritable.class, 
        AverageTemperatureReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);
    
    // Transmit threshold to mappers via job configuration
    wordCount.getConfiguration().set(QUALITY_PARAM_NAME, Double.toString(minimumQuality));
    
    wordCount.waitForCompletion(true);
    
    return 0;
  }
  
  static class AverageTemparatureMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private static double minimumQuality;
    private Text compositeKey = new Text();
    
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException {
      
      // Load settings
      minimumQuality = Double.parseDouble(
          context.getConfiguration().get(QUALITY_PARAM_NAME));
    }
    
    @Override
    protected void map(Object key, Text value,
        Context context) throws IOException,
        InterruptedException {
      
      String[] fields = value.toString().split("\t");
      
      double quality = Double.parseDouble(fields[3]);
      if (quality >= minimumQuality) {
        // emit composite key
        compositeKey.set(fields[0] + "\t" + fields[1]);
        context.write(compositeKey, new IntWritable(Integer.parseInt(fields[2])));
      }
    }
  }
  
  static class AverageTemperatureReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException,
        InterruptedException {
      
      int sum = 0;
      int count = 0;
      for (IntWritable i : values) {
        count++;
        sum += i.get();
      }
      double average = ((double)sum) / count;
      
      context.write(key, new DoubleWritable(average));
    }
  }
}