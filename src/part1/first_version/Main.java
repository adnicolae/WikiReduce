package part1.first_version;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class that creates a job process that is submitted to the hadoop environment
 * Run: WikiReduce.jar part1.first_version.Main N timestamp1 timestamp2 input
 * final-output
  */
public class Main extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("wiki-part1-first-version");

        job.setJarByClass(Main.class);

        // set the data types of the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set configuration attributes based on terminal arguments
        job.getConfiguration().set("N-value", args[0]);
        job.getConfiguration().set("timestamp1", args[1]);
        job.getConfiguration().set("timestamp2", args[2]);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
//        job.setCombinerClass(Combine.class);

        Path inputFilePath = new Path
                (args[3]);

        Path outputFilePath = new Path
                (args[4]);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }
}
