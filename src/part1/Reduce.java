package part1;

import helpers.MapHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Text, IntWritable> count = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int revisions = 0;

        for (IntWritable value : values) {
            revisions += value.get();
        }

        count.put(new Text(key), new IntWritable(revisions));
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