package de.tuberlin.dima.aim3.assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import javassist.expr.NewArray;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.jasper.compiler.SmapStratum.LineInfo;

import com.google.common.collect.ComparisonChain;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class AverageTemperaturePerMonthPact implements PlanAssembler, PlanAssemblerDescription {

  public static final double MINIMUM_QUALITY = 0.25;

  @Override
  public String getDescription() {
    return null;
  }
  
  public static class LineInputFormat extends TextInputFormat<PactNull, PactString> {

    @Override
    public boolean readLine(KeyValuePair<PactNull, PactString> pair, byte[] line) {
      
      // Pair is an output parameter
      pair.setKey(new PactNull());
      pair.setValue(new PactString(new String(line)));
      
      return true;
    }
    
  }
  
  public static class StringDoubleOutputFormat extends TextOutputFormat<YearMonthKey, PactDouble> {

    @Override
    public byte[] writeLine(KeyValuePair<YearMonthKey, PactDouble> pair) {
      // Build the string we want to write to file
      String outputString = pair.getKey().getYear() 
          + "\t" + pair.getKey().getMonth() 
          + "\t" + pair.getValue().toString();
      return outputString.getBytes();
    }
  }

  @Override
  public Plan getPlan(String... args) throws IllegalArgumentException {
    // Parse job options
    // The test method does not use this method, so not sure what arguments we should support
    int dop = (args.length > 0 ? Integer.parseInt(args[0]) : 1);    // Degree of parallelism, default 1
    String sourceFile = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");
    
    FileDataSourceContract<PactNull, PactString> source = new FileDataSourceContract<PactNull, PactString>(LineInputFormat.class, sourceFile);
    FileDataSinkContract<YearMonthKey, PactDouble> sink = new FileDataSinkContract<YearMonthKey, PactDouble>(StringDoubleOutputFormat.class, output);
    
    MapContract<PactNull, PactString, YearMonthKey, PactInteger> map = new MapContract<PactNull, PactString, AverageTemperaturePerMonthPact.YearMonthKey, PactInteger>(TemperaturePerYearAndMonthMapper.class);
    ReduceContract<YearMonthKey, PactInteger, YearMonthKey, PactDouble> reduce = new ReduceContract<AverageTemperaturePerMonthPact.YearMonthKey, PactInteger, AverageTemperaturePerMonthPact.YearMonthKey, PactDouble>(TemperatePerYearAndMonthReducer.class);

    source.setDegreeOfParallelism(dop);
    sink.setDegreeOfParallelism(dop);
    map.setDegreeOfParallelism(dop);
    reduce.setDegreeOfParallelism(dop);
    
    sink.setInput(reduce);
    reduce.setInput(map);
    map.setInput(source);
    
    return new Plan(sink, "Average Temperature Per Month");
  }
  
  public static class TemperaturePerYearAndMonthMapper
      extends MapStub<PactNull, PactString, YearMonthKey, PactInteger> {

    @Override
    public void map(PactNull pactNull, PactString line, Collector<YearMonthKey, PactInteger> collector) {
    	double minimumQuality = 0;
    	String[] fields = line.toString().split("\t");
        System.out.println("MAP");
        double quality = Double.parseDouble(fields[3]);
        if (quality >= minimumQuality) {
          // emit composite key
          YearMonthKey yearMonthKey = new YearMonthKey(Short.parseShort(fields[0]), Short.parseShort(fields[1]));
          collector.collect(yearMonthKey, new PactInteger(Integer.parseInt(fields[2])));
        }
    }
  }

  public static class TemperatePerYearAndMonthReducer
      extends ReduceStub<YearMonthKey, PactInteger, YearMonthKey, PactDouble> {

    @Override
    public void reduce(YearMonthKey yearMonthKey, Iterator<PactInteger> temperatures,
        Collector<YearMonthKey, PactDouble> collector) {
      
      int sum = 0;
      int count = 0;
      while (temperatures.hasNext()) {
        count++;
        sum += temperatures.next().getValue();
      }
      double average = ((double)sum) / count;
      
      collector.collect(yearMonthKey, new PactDouble(average));
    }
  }

  public static class YearMonthKey implements Key {
    
    short year;
    short month;

    public YearMonthKey() {}

    public YearMonthKey(short year, short month) {
      this.year = year;
      this.month = month;
    }

    @Override
    public int compareTo(Key other) {
      YearMonthKey otherCasted = (YearMonthKey) other;
      return ComparisonChain.start().compare(year, otherCasted.getYear()).compare(month, otherCasted.getMonth()).result();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(year);
      out.writeShort(month);
    }

    @Override
    public void read(DataInput in) throws IOException {
      year = in.readShort();
      month = in.readShort();
    }

    //IMPLEMENT equals() and hashCode()
    
    @Override
    public String toString() {
      return year + "\t" + month;
    }
    
    public short getYear() {
      return year;
    }
    
    public short getMonth() {
      return month;
    }
  }
}