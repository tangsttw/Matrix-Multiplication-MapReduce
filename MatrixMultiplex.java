package mmd.hw;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class MatrixMultiplex
{
    Integer inputSize = new Integer(500);

    // Mapper
    public static class Matrix_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context mapOutput) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splitLineInput = line.split(",");

            Text outputKey = new Text();
            Text outputValue = new Text();

            if (splitLineInput[0].equals("M")){
                for (int k=0; k<inputSize; k++){
                    outputKey.set(splitLineInput[1] + "," + k);
                    outputValue.set(splitLineInput[0] + "," + splitLineInput[2] + "," + splitLineInput[3]);
                    mapOutput.write(outputKey, outputValue);
                }
            } else{
                for (int i=0; i<inputSize; i++){
                    outputKey.set(i + "," + splitLineInput[2]);
                    outputValue.set(splitLineInput[0] + "," + splitLineInput[1] + "," + splitLineInput[3]);
                    mapOutput.write(outputKey, outputValue);
                }
            }
        }
    }

    // Reducer
    public static class Matrix_Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context combineOutput) throws IOException, InterruptedException {
            String[] value, nMatch;

            ArrayList<String> arrayM = new ArrayList<String>();
            ArrayList<String> arrayN = new ArrayList<String>();

            Integer product = new Integer(0);

            for (Text val : values) {
                value = val.toString().split(",");

                if (value[0].equals("M")) {
                    arrayM.add(value[1]);
                    arrayM.add(value[2]);
                } else {
                    arrayN.add(value[1]);
                    arrayN.add(value[2]);
                }
            }

            for (int i=0; i<arrayM.size(); i+=2) {
                for (int j=0; j<arrayN.size(); j+=2) {
                    if (arrayM.get(i).equals(arrayN.get(j))) {
                        product += Integer.parseInt(arrayM.get(i+1))*Integer.parseInt(arrayN.get(j+1));
                    }
                }
            }

            combineOutput.write(key, new Text(product.toString()));
        }
    }

    // main function
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: matrix multiplication <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "matrix multi");
        job.setJarByClass(MatrixMultiplex.class);

        job.setMapperClass(Matrix_Mapper.class);
        job.setReducerClass(Matrix_Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
