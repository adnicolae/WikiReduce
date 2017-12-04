package part1.first_version;

import helpers.MapHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;

/**
 * Class to implement a reducer for the MapReduce job.
 * Takes the output of the mapper <user_id, number of revisions>
 * and aggregates the number of revisions in the case of the First Version
 * then adds the user id and the number of revisions to a revisions HashMap
 * which is sorted by key then by value after the reducer has received all its data.
 *
 * The same reducer is used for part2, only that the key is the article id.
 */
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Text, IntWritable> count = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int revisions = 0;

        // Aggregate revisions for the first version
        // Does not affect performance of the improved version
        for (IntWritable value : values) {
            revisions += value.get();
        }

        count.put(new Text(key), new IntWritable(revisions));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, IntWritable> sortedMap = MapHelper.sortByValue(MapHelper.sortByKey(count));

        int N = Integer.parseInt(context.getConfiguration().get("N-value"));

        // Only output the first N key-value pairs
        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == N) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }
    }
}