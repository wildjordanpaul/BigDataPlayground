package smartflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class MovieMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, UserRating> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, UserRating> output, Reporter reporter) throws IOException {
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while(itr.hasMoreTokens()) {
            Text user = new Text(itr.nextToken());
            Text movie = new Text(itr.nextToken());
            IntWritable rating = new IntWritable(Integer.parseInt(itr.nextToken()));
            output.collect(movie, new UserRating(user, rating));
        }
    }
}
