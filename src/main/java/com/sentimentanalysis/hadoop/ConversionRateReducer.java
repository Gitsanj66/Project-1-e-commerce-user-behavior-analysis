package com.sentimentanalysis.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ConversionRateReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int interactions = 0;
        int purchases = 0;

        // Count the number of interactions and purchases
        for (Text value : values) {
            if (value.toString().equals("INTERACTION")) {
                interactions++;
            } else if (value.toString().equals("PURCHASE")) {
                purchases++;
            }
        }

        // Calculate the conversion rate
        double conversionRate = (interactions > 0) ? ((double) purchases / interactions) : 0.0;
        context.write(key, new DoubleWritable(conversionRate));
        }
}