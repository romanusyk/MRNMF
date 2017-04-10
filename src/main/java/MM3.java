import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by romm on 03.04.17.
 */
public class MM3 {

    private static class MM3Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int k = context.getConfiguration().getInt("k", -1);

            double[] ai = new double[k];
            String[] values = value.toString().split("\\t")[1].split(",");

            for (int j = 0; j < k; j++) {
                ai[j] = Double.parseDouble(values[j]);
            }

            for (int j = 0; j < k; j++) {
                LongWritable newKey = new LongWritable(j + 1);
                StringBuilder result = new StringBuilder();
                for (int l = 0; l < k; l++) {
                    result.append(ai[j] * ai[l]);
                    if (l < k - 1) {
                        result.append(",");
                    }
                }
                context.write(newKey, new Text(result.toString()));
            }
        }
    }

    private static class MM3Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {

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

            StringBuilder res = new StringBuilder();

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

    public MM3(Configuration configuration, String inputPath, String outputPath) {
        this.configuration = configuration;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(configuration, "MM3");

        job.setJarByClass(MRNMF.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MM3Mapper.class);
        job.setReducerClass(MM3Reducer.class);

        job.waitForCompletion(true);
    }
    
}
