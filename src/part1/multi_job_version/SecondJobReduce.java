package part1.multi_job_version;

import helpers.MapHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to implement the second reducer in a chained job.
 * It parses the files from the first reducer send through the second mapper
 * and outputs the top N results.
 */
public class SecondJobReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private java.util.Map<Text, IntWritable> count = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            count.put(new Text(key), new IntWritable(value.get()));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedMap = MapHelper.sortByValue(MapHelper.sortByKey(count));

        int N = Integer.parseInt(context.getConfiguration().get("N-value"));

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == N) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }
    }
}