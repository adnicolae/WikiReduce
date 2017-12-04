package part2.first_version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Class to implement a mapper for the MapReduce job.
 * The mapper takes a line and its content as an input and
 * outputs the article id found on the "REVISION" line and the number 1
 * to represent one revision of that article id.
 */
public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Configuration configuration = context.getConfiguration();

        try {
            if (value.charAt(0) == 'R') {
                String[] data = line.split(" ");
                String articleId = data[1];

                SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

                Date revisionTimestamp=simpleDateFormat.parse(data[4]);
                Date timestamp1 = simpleDateFormat.parse(configuration.get("timestamp1"));
                Date timestamp2 = simpleDateFormat.parse(configuration.get("timestamp2"));

                // Check if the timestamp of the current revision is within the given timestamps
                if (revisionTimestamp.after(timestamp1) && revisionTimestamp.before(timestamp2)) {
                    context.write(new Text(articleId), new IntWritable(1));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
