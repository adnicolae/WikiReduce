package part1.improved_version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ImprovedMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Map<Text, IntWritable> count = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Configuration configuration = context.getConfiguration();

        try {
            if (value.charAt(0) == 'R') {
                String[] data = line.split(" ");
                String userId = data[6];

                SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

                Date revisionTimestamp=simpleDateFormat.parse(data[4]);
                Date timestamp1 = simpleDateFormat.parse(configuration.get("timestamp1"));
                Date timestamp2 = simpleDateFormat.parse(configuration.get("timestamp2"));
                Text user = new Text(userId);

                if (revisionTimestamp.after(timestamp1) && revisionTimestamp.before(timestamp2)) {
                    if (count.containsKey(user)){
                        count.put(user, new IntWritable(count.get(user).get() + 1));
                    }
                    else {
                        count.put(user, new IntWritable(1));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Text key: count.keySet()) {
            context.write(new Text(key), new IntWritable(count.get(key).get()));
        }
    }
}
