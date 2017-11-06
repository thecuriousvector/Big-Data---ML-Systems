import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.examples.SecondarySort.IntPair;

  public class AverageReducer
       extends Reducer<IntWritable, IntPair, Text,DoubleWritable> {
    private Text average = new Text();
    private DoubleWritable avgDouble = new DoubleWritable();
    public void reduce(IntWritable key, Iterable<IntPair> pairs,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int count = 0; 
      for (IntPair pair: pairs) {
      	count += pair.getFirst();
    	 sum += pair.getSecond();
      }
      double prod = (double) sum;
      double avg = prod/count;
     // if (count != 0)
     // avg = avg/count;
      average.set("Average:");
      avgDouble.set(avg);
    	context.write(new Text("Sum"), new DoubleWritable(sum));
	context.write(new Text("Count"),new DoubleWritable (count));
      context.write(average, avgDouble);
    }
  }
