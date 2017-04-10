import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
 * Created by romm on 03.04.17.
 */
public class MM2 {

    private static class MM2Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text> {

        private int i;
        private int j;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            boolean transposeX = context.getConfiguration().getBoolean("transposeX", true);
            i = transposeX ? 0 : 1;
            j = transposeX ? 1 : 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\\t");
            context.write(new LongWritable(Long.parseLong(vals[i])), new Text(vals[j] + "\t" + vals[2]));
        }

    }

    private static class MM2Reducer1 extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int k = context.getConfiguration().getInt("k", -1);

            String fpath = context.getConfiguration().get("mpath");
            String wd = context.getConfiguration().get("od");
            Path file = new Path(wd, fpath);

            FileSystem fs = file.getFileSystem(new Configuration());
            FSDataInputStream ds = fs.open(file);
            long start = (key.get() - 1) * 8 * (k + 1);
            ds.seek(start + 8);

            double[] wi = new double[k];
            for (int counter = 0; counter < k; counter++) {
                wi[counter] = ds.readDouble();
            }

            ds.close();

            for (Text value : values) {
                String[] keyVal = value.toString().split("\\t");
                long j = Long.parseLong(keyVal[0]);
                double aij = Double.parseDouble(keyVal[1]);

                StringBuilder result = new StringBuilder();
                for (int cou = 0; cou < k; cou++) {
                    result.append(aij * wi[cou]);
                    if (cou < k) {
                        result.append(",");
                    }
                }
                context.write(new LongWritable(j), new Text(result.toString()));
            }
        }

    }

    private static class MM2Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            context.write(new LongWritable(Long.parseLong(vals[0])), new Text(
                    vals[1]));
        }

    }

    private static class MM2Reducer2 extends Reducer<LongWritable, Text, LongWritable, Text> {

        String prefix;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            prefix = context.getConfiguration().get("prefix", "");
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int k = context.getConfiguration().getInt("k", -1);

            double[] result = new double[k];

            for (Text value : values) {
                String[] ai = value.toString().split(",");
                for (int j = 0; j < k; j++) {
                    result[j] += Double.parseDouble(ai[j]);
                }
            }

            StringBuilder res = new StringBuilder(prefix);

            for (int i = 0; i < k; i++) {
                res.append(result[i]);
                if (i < k) {
                    res.append(",");
                }
            }
            context.write(key, new Text(res.toString()));
        }

    }

    private Configuration configuration;
    private String inputPath;
    private String outputPath;

    public MM2(Configuration configuration, String inputPath, String outputPath) {
        this.configuration = configuration;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(configuration);

        Path tmpPath = new Path(configuration.get("wd"), "tmp");

        job.setJarByClass(MRNMF.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MM2Mapper1.class);
        job.setReducerClass(MM2Reducer1.class);

        job.waitForCompletion(true);

        job = Job.getInstance(configuration);

        job.setJarByClass(MRNMF.class);

        FileInputFormat.addInputPath(job, tmpPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MM2Mapper2.class);
        job.setReducerClass(MM2Reducer2.class);

        job.waitForCompletion(true);
    }
}
