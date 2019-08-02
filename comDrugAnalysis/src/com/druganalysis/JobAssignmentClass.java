package com.druganalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 *This class will assign the job of mapReduce to hadoop.
 * Assigning mapper and reducer and taking inputs and resulting the outputs.
 */

public class JobAssignmentClass {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 2){   // Exactly 2 inputs, inputFile(Data) and outputFile.
            System.out.println("Usage: inputFile outputFile");
            System.exit(0);
        }


        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration,"Condition Drug Mapping");

        job.setJarByClass(JobAssignmentClass.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(ConditionDrugMapper.class);
        job.setReducerClass(ConditionDrugReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
