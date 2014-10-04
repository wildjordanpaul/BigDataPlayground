package smartflix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserRating implements Writable {

    private Text userId;
    private IntWritable rating;

    public UserRating(Text userId, IntWritable rating) {
        this.userId = userId;
        this.rating = rating;
    }

    public void write(DataOutput output) throws IOException {
        userId.write(output);
        rating.write(output);
    }

    public void readFields(DataInput input) throws IOException{
        userId.readFields(input);
        rating.readFields(input);
    }

    public Text getUser() {
        return userId;
    }

    public IntWritable getRating() {
        return rating;
    }

    public int hashCode() {
        return (userId.hashCode() ^ (rating.hashCode() >>> 8));
    }
}
