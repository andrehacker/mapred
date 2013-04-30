package de.tuberlin.dima.aim3.assignment3;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Map;


public class MatrixTransposition extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path corpusAsMatrix = new Path(parsedArgs.get("--matrix"));
    Path transposedCorpus = new Path(parsedArgs.get("--output"));

    Job vectorize = prepareJob(inputPath, corpusAsMatrix, TextInputFormat.class, VectorizeSentencesMapper.class,
        IntWritable.class, SparseVector.class, SequenceFileOutputFormat.class);
    vectorize.waitForCompletion(true);

    //TODO transpose the corpus matrix and write it to output

    return 0;
  }
}
