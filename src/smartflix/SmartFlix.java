package smartflix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class SmartFlix {

    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf job1 = new JobConf(SmartFlix.class);
        JobConf job2 = new JobConf(SmartFlix.class);

        // configure IO
        job1.set("targetUser", args[0]);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        // configure job1
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapperClass(MovieMapper.class);
        job1.setReducerClass(MovieReducer.class);
        job1.setCombinerClass(MovieReducer.class);

        //configure job2
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setReducerClass(PointReducer.class);
        job2.setCombinerClass(PointReducer.class);

        runJob(client, job1);
        runJob(client, job2);
    }

    private static void runJob(JobClient client, JobConf job) {
        client.setConf(job);
        try {
            JobClient.runJob(job);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
