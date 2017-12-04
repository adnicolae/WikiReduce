package part1.first_version;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to implement a combiner that would reduce the output of the mappers
 * by aggregating the total number of revisions a user has made.
 */
public class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int revisions = 0;

        for (IntWritable value : values){
            revisions += value.get();
        }

        context.write(key, new IntWritable(revisions));
    }
}
