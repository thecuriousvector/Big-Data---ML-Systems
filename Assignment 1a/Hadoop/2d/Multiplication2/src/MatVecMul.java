import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatVecMul extends Configured implements Tool {
	
		private Long noOfCols = (long) 0;
		
        @Override
        public int run(String[] args) throws Exception {
        	 Configuration conf = new Configuration();
        	 Integer noOfStripes = 3;
             conf.set("noOfStripes",noOfStripes.toString());
             String newFilePath = args[1].substring(0, args[1].lastIndexOf("/"));
             newFilePath = newFilePath + "/Vectors/";
             conf.setStrings("stripsPath", newFilePath);
             Long noOfColsPerStripe = splitVector(args, conf);
             conf.setStrings("colsPerStripe", noOfColsPerStripe.toString());
             Job job = new Job(conf, "Matrix-Vetor Multiplication (Vector Does Not Fit In Memory)");
             job.setJarByClass(MatVecMul.class);
             job.setMapperClass(MatVecMulMapper.class);
             job.setCombinerClass(MatVecMulReducer.class);
             job.setReducerClass(MatVecMulReducer.class);
             job.setInputFormatClass(TextInputFormat.class);
             job.setMapOutputKeyClass(LongWritable.class);
             job.setMapOutputValueClass(DoubleWritable.class);
             
              FileInputFormat.addInputPath(job, new Path(args[0]));
             FileInputFormat.addInputPath(job, new Path(args[1]));
             FileOutputFormat.setOutputPath(job, new Path(args[2]));
             return (job.waitForCompletion(true) ? 0 : -1);
    }
        
    public Long splitVector(String[] args, Configuration conf) throws Exception{

    	Long noOfColsPerStripe = (long) 0;
    	try{
            Path vectorFilePath = new Path(args[1]);
            String newFilePath = args[1].substring(0, args[1].lastIndexOf("/"));
            String newVecPath = newFilePath + "/Vectors/";
            FileSystem fs = FileSystem.get(conf);
            Path vectorStripsDir = new Path(newVecPath);
    		if (fs.exists(vectorStripsDir)) {
    			fs.delete(vectorStripsDir, true);
    		}
    		fs.mkdirs(vectorStripsDir);
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(fs.open(vectorFilePath)));
            String line1 = null;
            Long tempCols = (long) 0;
            while (((line1 = reader1.readLine()) != null) && noOfCols == 0){
            	String[] inputs = StringUtils.getStrings(line1);
            	Long currentCol = Long.valueOf(inputs[0]); 
            	if (currentCol > tempCols)
            		tempCols = currentCol;
            }
            reader1.close();
            noOfCols = tempCols;
            Long noOfStripes = Long.parseLong(conf.get("noOfStripes"));
            noOfColsPerStripe = noOfCols/noOfStripes;
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(vectorFilePath)));
            String line = null;
            for (int i = 1; i <= noOfStripes; i++) {
        		line = reader.readLine();
            	Path path = new Path(newVecPath + i);
				FSDataOutputStream outputStream;
				if (fs.exists(path)) {
					outputStream = fs.append(path);
				} else {
					outputStream = fs.create(path);
				}
	            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
            	for (int j = 1; j <= noOfColsPerStripe; j++) {
                	if (line != null) {
                		writer.write(line);
                		writer.newLine();
                	}
                	writer.close();
                }
            }
            reader.close();
            
        }catch(Exception e){
        }
    	return noOfColsPerStripe;
    }
    public static void main(String[] args) throws Exception {
    if (args.length != 3) {
            System.out.println("Prolduct:");
            System.exit(-1);
     }
    int result = ToolRunner.run(new Configuration(), new MatVecMul(), args);
    System.exit(result);
    }

}