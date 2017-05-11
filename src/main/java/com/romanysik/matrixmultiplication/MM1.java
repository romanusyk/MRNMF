package com.romanysik.matrixmultiplication;

import com.romanysik.MRNMF;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * 1-job matrix multiplication
 * share second matrix between all mappers, so it shouldn't be too large
 *
 * See MM-1 in http://cake.fiu.edu/Publications/Sun+al-10-LM.Large-Scale.Matrix.Factorization.using.MapReduce.ICDMW2010.published.paper.pdf
 * for details
 *
 * Created by romm on 03.04.17.
 */
public class MM1 {

    private static class MM1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private double[][] B;
        private int Bh;
        private int Bw;
        String prefix;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String BPath = context.getConfiguration().get("mpath");
            Bw = context.getConfiguration().getInt("mw", -1);
            Bh = context.getConfiguration().getInt("mh", -1);
            prefix = context.getConfiguration().get("prefix", "");

            Path pt = new Path(BPath);
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            FileSystem fs = FileSystem.get(conf);

            if (fs.isDirectory(pt)) {
                B = readMatrixFromOutput(pt, Bh, Bw);
            } else {
                B = new double[Bh][Bw];
                readMatrixFromFile(fs, pt, B);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] keyVal = value.toString().split("\\t");
            double[] Ai = new double[Bh];
            int i = Integer.parseInt(keyVal[0]) - 1;
            String[] values = keyVal[1].split(",");
            for (int j = 0; j < values.length; j++) {
                Ai[j] = Double.parseDouble(values[j]);
            }
            double[] Ci = new double[Bw];
            StringBuilder result = new StringBuilder(prefix);

            for (int j = 0; j < Bw; j++) {
                Ci[j] = 0d;
                for (int k = 0; k < Bh; k++) {
                    Ci[j] += Ai[k] * B[k][j];
                }
                result.append(Ci[j]);
                if (j != Bw - 1) {
                    result.append(",");
                }
            }
            context.write(new IntWritable(i + 1), new Text(result.toString()));
        }
    }

    private static void readMatrixFromFile(FileSystem fs, Path p, double[][] a) throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(p);
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] keyVal = line.split("\\t");
            int i = Integer.parseInt(keyVal[0]) - 1;
            int j = 0;
            for (String aij : keyVal[1].split(",")) {
                a[i][j++] = Double.parseDouble(aij);
            }
        }
        bufferedReader.close();
        inputStreamReader.close();
        fsDataInputStream.close();
    }

    public static double[][] readMatrixFromOutput(Path dir, int n, int m) throws IOException {

        double[][] a = new double[n][m];

        Configuration conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        FileSystem fs = dir.getFileSystem(conf);
        for (Path p : FileUtil.stat2Paths(fs.listStatus(dir))) {
            if (p.toString().contains("part")) {
                readMatrixFromFile(fs, p, a);
            }
        }

        return a;

    }
    
    private Configuration configuration;
    private String inputPath;
    private String outputPath;

    /**
     *
     * @param configuration with required params :
     *                      {
     *                          mpath: path to second matrix, which will be shared in memory,
     *                          mw: second matrix column number,
     *                          mh: second matrix row number,
     *                          prefix: for each line of result
     *                      }
     * @param inputPath - path to first matrix, which should be dense
     * @param outputPath
     */
    public MM1(Configuration configuration, String inputPath, String outputPath) {
        this.configuration = configuration;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(configuration, "com.romanysik.matrixmultiplication.MM1");

        job.setJarByClass(MRNMF.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MM1Mapper.class);

        job.waitForCompletion(true);
    }
    
}
