import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatVecMulMapper extends Mapper<LongWritable,Text,LongWritable,DoubleWritable> {
	
		private int stripNo = 1;
		private Long colCount = (long) 1;
		Map<Long,Double> vector = new HashMap<Long,Double>();
		
		@Override
        protected void setup(Context context) throws IOException, InterruptedException {
			Long colsPerStripe = (long) 0;
    		Configuration configuration = context.getConfiguration();
    		if (configuration != null)
    			colsPerStripe = configuration.getLong("colsPerStripe",1);
    		if (colCount == colsPerStripe+1) {
    			stripNo++;
    			colCount = (long) 1;
    		}
    		FileSystem fs = FileSystem.get(configuration);
    		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(configuration.getStrings("stripsPath")[0] + "/" + stripNo))));
    		String line = null;
            try {
                    while ((line = reader.readLine()) != null) {
                            String[] cols = StringUtils.getStrings(line);
                            vector.put(Long.valueOf(cols[0]), Double.valueOf(cols[1]));
                    }
            } finally {
            	   colCount++;
                   reader.close();
            }
        }

		 @Override
	        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        		String[] cols = StringUtils.getStrings(value.toString());
	                Long row = Long.valueOf(cols[0]);
	                Long col = Long.valueOf(cols[1]);
	                Double entry = Double.valueOf(cols[2]);
	                if (vector.containsKey(col)) {
	                        entry = entry * vector.get(col);
	                        context.write(new LongWritable(row), new DoubleWritable(entry));
	                }
	        }
}
                                                                                                                                                                                      
