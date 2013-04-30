package de.tuberlin.dima.aim3.assignment1;

import com.google.common.base.Joiner;
import de.tuberlin.dima.aim3.HadoopJob;
import de.tuberlin.dima.aim3.assignment1.AverageTemperaturePerMonth.AverageTemparatureMapper;
import de.tuberlin.dima.aim3.assignment1.AverageTemperaturePerMonth.AverageTemperatureReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class SecondarySortBookSort extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    // in a real distributed setting, we would need to include a TotalOrderPartitioner and sample the input,
    // for the sake of simplicity, we omit this here
    
    Job job = prepareJob(inputPath, outputPath, TextInputFormat.class, 
        ByCenturyAndTitleMapper.class, BookSortKey.class, Text.class, 
        SecondarySortBookSortReducer.class, Text.class, NullWritable.class, TextOutputFormat.class);
    
    job.waitForCompletion(true);
   
    return 0;
  }

  static class ByCenturyAndTitleMapper extends Mapper<Object,Text,BookSortKey,Text> {
    
    private final static Text empty = new Text("");

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      
      String[] fields = line.toString().split("\t");
      
      // Create and emit a composite key
      // I leave the value empty, not sure why the reducer emits it as well
      BookSortKey keyOut = new BookSortKey(fields[1].substring(0, 2), fields[2]);
      ctx.write(keyOut, empty);
      
      // Note: This won't work if the same title exists for multiple centuries
    }
  }

  static class SecondarySortBookSortReducer extends Reducer<BookSortKey,Text,Text,NullWritable> {
    @Override
    protected void reduce(BookSortKey bookSortKey, Iterable<Text> values, Context ctx)
        throws IOException, InterruptedException {
      
      for (Text value : values) {
        String out = Joiner.on('\t').skipNulls().join(new Object[] { bookSortKey.toString(), value.toString() });
        ctx.write(new Text(out), NullWritable.get());
      }
      
    }
  }

  static class BookSortKey implements WritableComparable<BookSortKey> {
    
    private String century;
    private String title;

    BookSortKey() {}
    
    BookSortKey(final String century, final String title) {
      this.century = century;
      this.title = title;
    }

    @Override
    public int compareTo(BookSortKey other) {
      // This will compare both parts of the composite key, for purpose of sorting
      int result = this.century.compareTo(other.getCentury());
      if (result == 0) {
        result = this.title.compareTo(other.getTitle());
      }
      return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, century);
      WritableUtils.writeString(out, title);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      century = WritableUtils.readString(in);
      title = WritableUtils.readString(in);
    }

    @Override
    public boolean equals(Object o) {
      BookSortKey other = (BookSortKey)o; 
      return (other.getTitle().equals(title) && other.getCentury().equals(century));
    }

    @Override
    public int hashCode() {
      // Considers both parts of the composite key
      // Default partitioner will send each century/title combination to a single reducer
      String combined = century + title;
      return combined.hashCode();
    }
    
    @Override
    public String toString() {
      return century + "\t" + title;
    }

    private String getCentury() {
      return century;
    }

    private String getTitle() {
      return title;
    }
  }

}