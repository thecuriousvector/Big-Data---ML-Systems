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

public class MatVecMul extends Configured implements Tool {

	
	@Override
  	public int run(String[] args) throws Exception {
   		 Configuration conf = new Configuration();
   		 Job job = new Job(conf, "Matrix-Vetor Multiplication (Vector Fits In Memory)");
	   	 job.setJarByClass(MatVecMul.class);
   		 job.setMapperClass(MatVecMulMapper.class);
    		 job.setReducerClass(MatVecMulReducer.class);
    		 job.setInputFormatClass(TextInputFormat.class);
   		 job.setMapOutputKeyClass(LongWritable.class);
    		 job.setMapOutputValueClass(DoubleWritable.class);
	
		 DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());
                 FileInputFormat.addInputPath(job, new Path(args[0]));
                 FileOutputFormat.setOutputPath(job, new Path(args[2]));

		 return (job.waitForCompletion(true) ? 0 : -1);
	}
	
	public static void main(String[] args) throws Exception {
	if (args.length != 3) {
      		System.out.println("Product:");
      		System.exit(-1);
   	 }
    	int result = ToolRunner.run(new Configuration(), new MatVecMul(), args);
    	System.exit(result);
	}
}

