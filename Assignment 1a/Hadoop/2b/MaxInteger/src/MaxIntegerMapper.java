import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class MaxIntegerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text largestInteger = new Text();
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	int max_map = 0;
      while (itr.hasMoreTokens()) {
	int temp = Integer.parseInt(itr.nextToken());
	if (temp > max_map)
		max_map = temp;
      }
        largestInteger.set("Largest integer is:");
        context.write(largestInteger, new IntWritable(max_map));
    }
  }
