package part1.multi_job_version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;
import part1.first_version.Reduce;
import part1.improved_version.ImprovedMap;

/**
 * Class that creates a multi job process that is submitted to the hadoop environment
 * Run: WikiReduce.jar part1.multi_job_version.MultiJobMain N timestamp1 timestamp2 input
 * intermediate-output final-output
 */
public class MultiJobMain extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf1=new Configuration();
        Job j1=Job.getInstance(conf1);

        j1.setJarByClass(MultiJobMain.class);

        j1.setMapperClass(ImprovedMap.class);
        j1.setReducerClass(Reduce.class);
        j1.setPartitionerClass(Partition.class);

        // Set the number of reducers for this tasks, for testing purposes
        j1.setNumReduceTasks(6);

        // Set configuration attributes based on terminal arguments
        j1.getConfiguration().set("N-value", args[0]);
        j1.getConfiguration().set("timestamp1", args[1]);
        j1.getConfiguration().set("timestamp2", args[2]);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);

        // Set the input and intermediate file paths
        Path inputFilePath = new Path
                (args[3]);

        Path intermediateFilePath = new Path
                (args[4]);

        FileInputFormat.addInputPath(j1, inputFilePath);
        FileOutputFormat.setOutputPath(j1, intermediateFilePath);

        // Wait for the first job to complete before starting the second job
        j1.waitForCompletion(true);

        /**
         * Job 2 configuration
         */
        Configuration conf2=new Configuration();
        Job j2=Job.getInstance(conf2);

        j2.setJarByClass(MultiJobMain.class);

        // Set the configuration attributes for the second job
        j2.getConfiguration().set("N-value", args[0]);

        // Set the Mapper and Reducer for the second job
        j2.setMapperClass(SecondJobMap.class);
        j2.setReducerClass(SecondJobReduce.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        // Set the final output path
        Path finalOutputPath=new Path
                (args[5]);

        FileInputFormat.addInputPath(j2, intermediateFilePath);
        FileOutputFormat.setOutputPath(j2, finalOutputPath);

        finalOutputPath.getFileSystem(conf2).delete(finalOutputPath, true);

        return j2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultiJobMain(), args);
        System.exit(exitCode);
    }
}
