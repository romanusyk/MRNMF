package com.romanysik;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import java.util.HashMap;
import java.util.Map;

/**
 * Created by romm on 04.04.17.
 */
public class MatrixUpdater {

    private static class UMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, NumberFormatException, InterruptedException {

            String[] vals = value.toString().split("\t");
            if (!vals[1].contains(":")) {
                vals[1] = "m:" + vals[1];
            }
            context.write(new LongWritable(Long.parseLong(vals[0])), new Text(vals[1]));

        }
    }

    private static class UReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private int k;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = context.getConfiguration().getInt("mw", -1);
        }

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            boolean sqrt = context.getConfiguration().getBoolean("sqrt", false);

            StringBuilder result = new StringBuilder();

            String[] arrayNames = new String[] {"m", "a", "b"};
            Map<String, double[]> arrays = new HashMap<>();
            for (String arrayName : arrayNames) {
                arrays.put(arrayName, new double[k]);
            }

            for (Text value : values) {
                String[] keyVal = value.toString().split(":");
                String[] xi = keyVal[1].split(",");
                for (int j = 0; j < k; j++) {
                    arrays.get(keyVal[0])[j] = Double.parseDouble(xi[j]);
                }
            }

            for (int j = 0; j < k; j++) {
                double frac = arrays.get("a")[j] / arrays.get("b")[j];
                if (sqrt) {
                    frac = Math.sqrt(frac);
                }
                result.append(arrays.get("m")[j] * frac);
                if (j != k - 1)
                    result.append(",");
            }

            context.write(key, new Text(result.toString()));
        }

    }

    public static void addInpuPath(Job job, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(new Configuration());
        if (fs.isDirectory(path)) {
            for (Path p : FileUtil.stat2Paths(fs.listStatus(path))) {
                if (p.toString().contains("part"))
                    FileInputFormat.addInputPath(job, p);
            }
        } else {
            FileInputFormat.addInputPath(job, path);
        }
    }

    private Configuration configuration;
    private String[] inputPaths;
    private String outputPath;
    private boolean sqrt;

    public MatrixUpdater(Configuration configuration, String[] inputPaths, String outputPath) {
        this.configuration = configuration;
        this.inputPaths = inputPaths;
        this.outputPath = outputPath;
        this.sqrt = false;
    }

    public MatrixUpdater(Configuration configuration, String[] inputPaths, String outputPath, boolean sqrt) {
        this.configuration = configuration;
        this.inputPaths = inputPaths;
        this.outputPath = outputPath;
        this.sqrt = sqrt;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {

        configuration.setBoolean("sqrt", sqrt);

        Job job = Job.getInstance(configuration, "com.romanysik.MatrixUpdater");

        job.setJarByClass(MRNMF.class);

        for (String path : inputPaths) {
            addInpuPath(job, new Path(path));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(UMapper.class);
        job.setReducerClass(UReducer.class);

        job.waitForCompletion(true);
    }

}
