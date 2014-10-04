package smartflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class PointReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    private int max = 0;
    private Text maxUser = new Text();

    @Override
    public void reduce(Text user, Iterator<IntWritable> pointIterator, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (pointIterator.hasNext()) {
            IntWritable points = pointIterator.next();
            sum += points.get();
        }

        if(sum > max) {
            max = sum;
            maxUser.set(user);
            output.collect(user, new IntWritable(sum));
        }

    }
}
