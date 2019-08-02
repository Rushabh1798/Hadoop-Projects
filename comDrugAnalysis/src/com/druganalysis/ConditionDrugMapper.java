package com.druganalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/**
 * DataSet used here is the UCI ML Drug Review.
 *This is our main Mapper class.
 * It will map the medical condition with the drugs those are used to cure it,
 * line by line as the Mapper does.
 * **/

public class ConditionDrugMapper extends Mapper<LongWritable , Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineData = value.toString().split("\\t");      //Tab delimited file

        if(lineData.length == 3){       // 3 columns in the dataSet(Handling missing values)
            context.write(new Text(lineData[2]), new Text(lineData[1])); // 3rd column is the condition | 2nd column is the drug used.
        }
    }
}
