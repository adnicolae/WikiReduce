package part1.first_version;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;

// creates a job process that is submitted to the hadoop environment
public class Main extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //
        Job job = Job.getInstance(getConf());
        job.setJobName("wiki-part1");
        // the jar where the main class is present in
        job.setJarByClass(Main.class);

        // set the data types of the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //"2005-12-06T17:44:47Z"
        job.getConfiguration().set("timestamp1", "2005-12-06T17:44:47Z");
        //"2006-03-24T02:14:46Z"
        job.getConfiguration().set("timestamp2", "2006-03-24T02:14:46Z");
        job.getConfiguration().set("N-value", "10");


        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
//        job.setCombinerClass(ImprovedCombiner.class);
        job.setNumReduceTasks(1);

        Path inputFilePath = new Path
                ("/home/andrei/hadoop-install/HadoopProjects/WikiReduce/data/input" +
                        "/wiki.txt");
//        Path inputFilePath = new Path
//                ("/home/andrei/hadoop-install/HadoopProjects/WikiReduce/data/test"+
//                        "-improved-map2");
        Path outputFilePath = new Path
                ("/home/andrei/hadoop-install/HadoopProjects/WikiReduce/data/output" +
                        "/part1_first-version");

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }
}
