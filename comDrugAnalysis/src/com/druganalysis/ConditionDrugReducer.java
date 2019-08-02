package com.druganalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 *This is our main Reducer class.
 * It will reduce the condition : drug pairs mapped by the Mapper class.
 */

public class ConditionDrugReducer extends Reducer<Text,Text, Text, Text > {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> drugList = new TreeSet<>(); // Taken care the repeated values and and sorting using TreeSet

        for (Text drug : values) {
            drugList.add(drug.toString());
        }
        context.write(key, new Text(drugList.toString()));
    }
}
