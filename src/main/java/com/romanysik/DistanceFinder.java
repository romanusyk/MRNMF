package com.romanysik;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by romm on 04.04.17.
 */
public class DistanceFinder {

    private static class DFMapper1 extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\t", 2);
            context.write(new LongWritable(Long.parseLong(vals[0])), new Text(vals[1]));
        }

    }

    private static class DFReducer1 extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int m = context.getConfiguration().getInt("mw", -1);

            double[] ai = new double[m];
            double[] bi = new double[m];

            int i = (int)key.get() - 1;

            for (Text value : values) {
                if (value.toString().contains("\t")) {
                    String[] keyVal = value.toString().split("\\t");
                    ai[Integer.parseInt(keyVal[0]) - 1] = Double.parseDouble(keyVal[1]);
                } else {
                    String[] vals = value.toString().split(",");
                    for (int j = 0; j < m; j++) {
                        bi[j] = Double.parseDouble(vals[j]);
                    }
                }

            }

            double difference = 0d;

            for (int j = 0; j < m; j++) {
                difference += Math.pow(ai[j] - bi[j], 2d);
            }
            context.write(new LongWritable(i + 1), new Text(difference + ""));

        }

    }

    private static class DFMapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\t", 2);
            context.write(new LongWritable(1), new Text(vals[1]));
        }

    }

    private static class DFReducer2 extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double sum = 0d;

            for (Text value : values) {
                sum += Double.parseDouble(value.toString());
            }

            double result = Math.sqrt(sum);
            System.out.println("\n\n\n\n\n" + result + "\n\n\n\n\n");
            context.write(new LongWritable(1), new Text(result + ""));

        }

    }

    private Configuration configuration;
    private String Xpath;
    private String FGPath;
    private String outputPath;

    public DistanceFinder(Configuration configuration, String XPath, String FGPath, String outputPath) {
        this.configuration = configuration;
        this.Xpath = XPath;
        this.FGPath = FGPath;
        this.outputPath = outputPath;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(configuration, "DF1");

        Path tmpPath = new Path(configuration.get("wd"), "dftmp");

        job.setJarByClass(MRNMF.class);

        FileInputFormat.addInputPath(job, new Path(Xpath));
        MatrixUpdater.addInpuPath(job, new Path(FGPath));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(DFMapper1.class);
        job.setReducerClass(DFReducer1.class);

        job.waitForCompletion(true);

        job = Job.getInstance(configuration, "DF2");

        job.setJarByClass(MRNMF.class);

        MatrixUpdater.addInpuPath(job, tmpPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(DFMapper2.class);
        job.setReducerClass(DFReducer2.class);

        job.waitForCompletion(true);
    }

}
