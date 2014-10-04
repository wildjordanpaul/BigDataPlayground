package smartflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MovieReducer extends MapReduceBase implements Reducer<Text, UserRating, Text, IntWritable> {
    private Text targetUser;

    @Override
    public void configure(JobConf job) {
        String input = job.get("targetUser");
        input = input == null || input.length() == 0 ? "1488844" : input;
        targetUser = new Text(input);
    }

    @Override
    public void reduce(Text text, Iterator<UserRating> userRatingIterator, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        IntWritable targetRating = null;
        List<UserRating> userRatingList = new ArrayList<UserRating>();

        while(userRatingIterator.hasNext()){
            UserRating userRating = userRatingIterator.next();
            if (userRating.getUser().equals(targetUser))
                targetRating = userRating.getRating();
            else
                userRatingList.add(userRating);
        }

        if(targetRating != null) {
            for (UserRating userRating : userRatingList) {
                output.collect(userRating.getUser(), calculatePoints(userRating.getRating(), targetRating));
            }
        }
    }

    private IntWritable calculatePoints(IntWritable userRating, IntWritable targetRating) {
        switch(Math.abs(userRating.get() - targetRating.get())) {
            case 0:
                return new IntWritable(5);
            case 1:
                return new IntWritable(3);
            case 2:
                return new IntWritable(1);
            case 3:
                return new IntWritable(-1);
            case 4:
                return new IntWritable(-3);
            case 5:
            default:
                return new IntWritable(-5);
        }
    }
}
