package part2.multi_job_version;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Class for the second mapper in a chained task. It parses the data sent from the first
 * reducer and sends it to the second reducer without altering it.
 */
public class SecondJobMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        String data[] = line.split("\t");
        String articleId = data[0];
        Integer revisions = Integer.parseInt(data[1]);

        context.write(new Text(articleId), new IntWritable(revisions));
    }
}
