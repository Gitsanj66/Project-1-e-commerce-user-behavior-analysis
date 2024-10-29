package com.sentimentanalysis.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductCategoryMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        if (fields.length == 5) {  // For user_activity.csv
            String productId = fields[3];
            context.write(new Text(productId), new Text("INTERACTION"));
        } else if (fields.length == 6) {  // For transactions.csv
            String productCategory = fields[2];
            context.write(new Text(productCategory), new Text("PURCHASE"));
            }
            }
}