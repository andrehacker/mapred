package de.tuberlin.dima.aim3.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PrimeNumbersWritable implements Writable {

  private int[] numbers;

  public PrimeNumbersWritable() {
    numbers = new int[0];
  }

  public PrimeNumbersWritable(int... numbers) {
    this.numbers = numbers;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // Transmit size first
    // I could also use EOFException, not sure if this is better
    out.writeInt(numbers.length);
    for (int i : numbers) {
      out.writeInt(i); 
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    numbers = new int[size];
    for (int i=0; i<size; ++i) { 
      numbers[i] = in.readInt();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PrimeNumbersWritable) {
      PrimeNumbersWritable other = (PrimeNumbersWritable) obj;
      return Arrays.equals(numbers, other.numbers);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(numbers);
  }
}