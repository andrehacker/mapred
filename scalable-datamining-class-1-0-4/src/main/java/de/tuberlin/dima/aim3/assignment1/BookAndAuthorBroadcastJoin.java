package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.base.Joiner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Joins books and authors on author-id
 * 
 * Strategy: Broadcast the smaller table to all mappers and join in mappers
 * 
 * Schemas
 * Books:   author-id, year-publication, book-title
 * Authors: author-id, name
 * 
 * @author André Hacker
 *
 */
public class BookAndAuthorBroadcastJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    // Add the smaller data set (books) to Hadoops distributed cache
    // Assumption: We should not implement the logic to choose the smaller dataset
    
    // Prepare Job
    // We don't use Identity Reducer because this would result in sorting all the keys
    // Instead we set the number of reduces to zero, which just executes the Map phase
    // This is done for us in the following prepareJob overload:
    Job job = prepareJob(books, outputPath, TextInputFormat.class, 
        BroadCastJoinMapper.class, Text.class, NullWritable.class, 
        TextOutputFormat.class);
    
    // Add authors to distributed cache
    DistributedCache.addCacheFile(new URI(authors.toString()), job.getConfiguration());
    
    job.waitForCompletion(true);

    return 0;
  }
  
  static class BroadCastJoinMapper extends Mapper<Object, Text, Text, NullWritable> {

    private HashMap<String, String> authorMap = new HashMap<String, String>();
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      
      // Read Authors from distributed cache and build an index / HashMap
      // Use Strings as HashMap key to avoid parsing (not sure if int would be faster)
      Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      BufferedReader br = new BufferedReader(new FileReader(files[0].toString()));
      String line;
      while ((line = br.readLine()) != null) {
        String fields[] = line.split("\t");
        authorMap.put(fields[0], fields[1]);
      }
      br.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
      
      String[] fields = value.toString().split("\t");
      
      // Lookup author-id in hash table
      // Assumption: This is a 1:n join, so for a single author-id there is one author, but n books
      String authorName = authorMap.get(fields[0]);
      if (authorName != null) {
        // Emit matched tuples as key only
        String out = Joiner.on('\t').join(new Object[] { 
            authorName,
            fields[2],
            fields[1] });
        context.write(new Text(out), NullWritable.get());
      } else {
        System.out.println("Debug: No matching author found for id " + fields[0]);
      }
      
    }
  }

}