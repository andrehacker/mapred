package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tools.ant.types.CommandlineJava.SysProperties;

import com.google.common.base.Joiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Joins books and authors on author-id
 * 
 * Strategy: (Re)partition both inputs by the join key.
 * Additionally use secondary sort to sort not only by book name, 
 * but additionally by tag (book or author)
 * 
 * Schemas
 * Books:   author-id, year-publication, book-title
 * Authors: author-id, name
 * 
 * @author André Hacker
 *
 */
public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));
    
    Job job = new Job(new Configuration(getConf()));
    Configuration jobConf = job.getConfiguration();
    
    //job.setJarByClass(JoinReducer.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    //jobConf.set("mapred.input.dir", authors.toString());
    MultipleInputs.addInputPath(job, authors, TextInputFormat.class, AuthorMapper.class);
    MultipleInputs.addInputPath(job, books, TextInputFormat.class, BooksMapper.class);
    
    //job.setMapperClass(AuthorMapper.class); // replaced by MultipleInputs
    job.setMapOutputKeyClass(TextPairKey.class);
    job.setMapOutputValueClass(Text.class);
    
    jobConf.setBoolean("mapred.compress.map.output", true);
    
    job.setReducerClass(JoinReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    
    // custom classes for secondary sorting
    job.setPartitionerClass(NaturalKeyPartitioner.class);
    job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

    //job.setJobName(getCustomJobName(job, AuthorMapper.class, JoinReducer.class));
    
    job.setOutputFormatClass(TextOutputFormat.class);
    jobConf.set("mapred.output.dir", outputPath.toString());
    
    job.waitForCompletion(true);
    
    return 0;
  }
  
  static abstract class JoinMapper extends Mapper<Object, Text, TextPairKey, Text> {
    
    /**
     * @return Return the tag for this
     */
    abstract String getTag();
    
    @Override
    protected void map(Object key, Text value,
        Context context) throws IOException,
        InterruptedException {
      
      String[] fields = value.toString().split("\t");
      
      // Tag the 
      TextPairKey keyOut = new TextPairKey(new Text(fields[0]), new Text(getTag()));
      
      // Transmit the tuple data as value
      // For simplicity we send the author-id again, which adds minor traffic
      context.write(keyOut, value);
      
    }
  }
  
  static class AuthorMapper extends JoinMapper {

    private static final String TAG = "author";
    
    @Override
    String getTag() { return TAG; }
    
  }
  
  static class BooksMapper extends JoinMapper {

    private static final String TAG = "book";
    
    @Override
    String getTag() { return TAG; }
    
  }
  
  static class JoinReducer extends Reducer<TextPairKey, Text, Text, NullWritable> {
    
    private Text out = new Text();  // single instance, to reduce allocations
    
    @Override
    protected void reduce(TextPairKey key, Iterable<Text> values,
        Context context) throws IOException,
        InterruptedException {
      
      // We know that we have a one-to-many join,
      // and we know that the author comes first
      // (secondary sorting on tag, so "author" comes before "books")
      // So we just pick the first value as author
      Iterator<Text> it = values.iterator();
      String author = it.next().toString().split("\t")[1];
      
      // Iterate over following book values. Every value is a match
      while (it.hasNext()) {
        String[] bookFields = it.next().toString().split("\t");
        out.set(Joiner.on("\t").join(new String[] {
            author, bookFields[2], bookFields[1]
          }));
        
        context.write(out, NullWritable.get());
      }
    }
    
  }
  
  /**
   * This Partitioner considers the natural key only (first part of composite key)
   * This makes sure that all values with the same natural key arrive at the same reduce-task
   */
  static class NaturalKeyPartitioner extends Partitioner<TextPairKey, Text> {

    @Override
    public int getPartition(TextPairKey pair, Text value, int numPartitions) {
      int hashCode = pair.getLeft().hashCode();
      return hashCode % numPartitions;
    }
    
  }
  
  /**
   * This comparator will be used as GroupingComparator
   * and make sure that only the first part of the composite key
   * is considered when grouping items together for a reduce call
   */
  static class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
      super(TextPairKey.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      // No other way than to cast
      TextPairKey first = (TextPairKey) a;
      TextPairKey second = (TextPairKey) b;
      return first.getLeft().toString().compareTo(second.getLeft().toString());
    }
    
  }
  
  static class TextPairKey implements WritableComparable<TextPairKey> {
    
    private Text left;
    private Text right;

    TextPairKey() {
      this.left = new Text();
      this.right = new Text();
    }
    
    TextPairKey(final Text left, final Text right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public int compareTo(TextPairKey other) {
      // This will compare both parts of the composite key, for purpose of sorting
      int result = this.left.compareTo(other.getLeft());
      if (result == 0) {
        result = this.right.compareTo(other.getRight());
      }
      return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      left.write(out);
      right.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      left.readFields(in);
      right.readFields(in);
    }

    @Override
    public int hashCode() {
      // Considers both parts of the composite key
      // Default partitioner will send each century/title combination to a single reducer
      String combined = left.toString() + right.toString();
      return combined.hashCode();
    }
    
    @Override
    public String toString() {
      return left + "\t" + right;
    }

    private Text getLeft() {
      return left;
    }

    private Text getRight() {
      return right;
    }
  }
  
}