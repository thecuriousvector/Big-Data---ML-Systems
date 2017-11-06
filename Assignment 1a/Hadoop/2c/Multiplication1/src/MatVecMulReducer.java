import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MatVecMulReducer extends
      Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable> {
    
    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, 
        Context context) throws IOException, InterruptedException {
      double sum = 0.0D;
      for (DoubleWritable value : values) {
      	sum = sum + value.get();
       }
      context.write(key, new DoubleWritable(sum)); 
    }
}
