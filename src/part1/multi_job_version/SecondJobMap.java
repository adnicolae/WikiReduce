package part1.multi_job_version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SecondJobMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        Configuration configuration = context.getConfiguration();

        String data[] = line.split("\t");
        String userId = data[0];
        Integer revisions = Integer.parseInt(data[1]);


        context.write(new Text(userId), new IntWritable(revisions));
    }
}
