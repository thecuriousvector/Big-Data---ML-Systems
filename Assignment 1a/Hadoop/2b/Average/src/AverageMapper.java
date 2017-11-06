import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.examples.SecondarySort.IntPair;

  public class AverageMapper
       extends Mapper<Object, Text, IntWritable, IntPair>{

    private static final IntWritable one = new IntWritable(1);
    //private int counter = 0;
    private IntPair countSum = new IntPair();	

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	int sum = 0;
	int counter = 0;
      while (itr.hasMoreTokens()) {
	int temp = Integer.parseInt(itr.nextToken());
	sum += temp;
	counter += 1;
      }
	//CountSumPair countSum = new CountSumPair(counter, sum);
	//IntPair countSum = new IntPair();
	countSum.set(counter,sum);
        context.write(one, countSum);
    }
  }
