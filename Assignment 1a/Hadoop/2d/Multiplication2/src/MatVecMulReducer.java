import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


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

