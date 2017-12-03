package part2.multi_job_version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;
import part1.first_version.Reduce;
import part1.multi_job_version.Partition;
import part1.multi_job_version.SecondJobReduce;

public class MultiJobMain extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf1=new Configuration();
        Job j1=Job.getInstance(conf1);

        j1.setJarByClass(MultiJobMain.class);

        j1.setMapperClass(part2.improved_version.ImprovedMap.class);
        j1.setReducerClass(Reduce.class);
        j1.setPartitionerClass(Partition.class);

        j1.setNumReduceTasks(4);

        //"2005-12-06T17:44:47Z"
        //2007-01-05T14:44:47Z
        j1.getConfiguration().set("timestamp1", "2005-12-06T17:44:47Z");
        //"2006-03-24T02:14:46Z"
        //2008-01-05T14:48:05Z
        j1.getConfiguration().set("timestamp2", "2006-03-24T02:14:46Z");
        j1.getConfiguration().set("N-value", "10");

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);

        Path inputFilePath = new Path
                ("/home/andrei/hadoop-install/HadoopProjects/WikiReduce/data/input" +
                        "/wiki.txt");

        Path outputFilePath = new Path
                ("/home/andrei/hadoop-install/HadoopProjects/WikiReduce/data/output" +
                        "/part2_multijob-version-intermediate");

        FileInputFormat.addInputPath(j1, inputFilePath);
        FileOutputFormat.setOutputPath(j1, outputFilePath);

        j1.waitForCompletion(true);


        Configuration conf2=new Configuration();
        Job j2=Job.getInstance(conf2);

        j2.setJarByClass(MultiJobMain.class);

        j2.getConfiguration().set("N-value", "10");

        j2.setMapperClass(SecondJobMap.class);
        j2.setReducerClass(SecondJobReduce.class);

        j2.setNumReduceTasks(1);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        Path finalOutputPath=new Path
                ("/home/andrei/hadoop-install/HadoopProjects/WikiReduce/data/output" +
                        "/part2_multijob-version-final");

        FileInputFormat.addInputPath(j2, outputFilePath);
        FileOutputFormat.setOutputPath(j2, finalOutputPath);

        finalOutputPath.getFileSystem(conf2).delete(finalOutputPath, true);

        return j2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultiJobMain(), args);
        System.exit(exitCode);
    }
}
