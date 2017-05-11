package com.romanysik.algorithm;

import com.romanysik.matrixmultiplication.MM1;
import com.romanysik.matrixmultiplication.MM2;
import com.romanysik.matrixmultiplication.MM3;
import com.romanysik.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by romm on 06.05.17.
 */
public class NMF implements Algorithm {

    private String inputFile;

    private int n;
    private int m;
    private int k;

    private String workingDirectory;
    private String outputDirectory;

    private Path wd;
    private Path od;

    private FileSystem wdfs;
    private FileSystem odfs;

    public NMF(String inputFile, int n, int m, int k, int r,
               String workingDirectory, String outputDirectory,
               Path wd, Path od,
               FileSystem wdfs, FileSystem odfs) throws IOException {

        this.inputFile = inputFile;
        this.n = n;
        this.m = m;
        this.k = k;
        this.workingDirectory = workingDirectory;
        this.outputDirectory = outputDirectory;
        this.wd = wd;
        this.od = od;
        this.wdfs = wdfs;
        this.odfs = odfs;

        BufferedFileWriter bufferedFileWriter = new BufferedFileWriter();

        // Init F matrix randomly
        bufferedFileWriter.open(odfs, new Path(od, "F.txt"));
        MatrixInitializer.writeRandomMatrix(n, k, r, bufferedFileWriter);
        bufferedFileWriter.close();

        // Init G matrix randomly
        bufferedFileWriter.open(odfs, new Path(od, "G.txt"));
        MatrixInitializer.writeRandomMatrix(m, k, r, bufferedFileWriter);
        bufferedFileWriter.close();

        MatrixByteConverter.txt2dat(od, "F.txt", "F.dat");
        MatrixByteConverter.txt2dat(od, "G.txt", "G.dat");

    }


    @Override
    public void compute(int iterations) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();

        configuration.set("wd", workingDirectory);
        configuration.set("od", outputDirectory);

        ResultHolder resultHolder = new ResultHolder(odfs, new Path(od, "report.txt"));

        for (int i = 0; i < iterations; i++) {

            // Clear working directory
            if (wdfs.exists(wd)) {
                wdfs.delete(wd, true);
            }
            wdfs.mkdirs(wd);

            // A = X.T.dot(F)
            configuration.setBoolean("transposeX", true);
            configuration.set("mpath", "F.dat");
            configuration.setInt("k", k);
            configuration.set("prefix", "a:");
            new MM2(configuration, inputFile, workingDirectory + "/A").run();

            // B = F.T.dot(F)
            new MM3(configuration, outputDirectory + "/F.txt", workingDirectory + "/mm3").run();

            // B = G.dot(B)
            configuration.set("mpath", workingDirectory + "/mm3");
            configuration.setInt("mw", k);
            configuration.setInt("mh", k);
            configuration.set("prefix", "b:");
            new MM1(configuration, outputDirectory + "/G.txt", workingDirectory + "/B").run();

            // Update G
            new MatrixUpdater(
                    configuration,
                    new String[]{
                            workingDirectory + "/A",
                            workingDirectory + "/B",
                            outputDirectory + "/G.txt"
                    },
                    workingDirectory + "/G"
            ).run();

            wdfs.delete(new Path(od, "G.txt"), true);
            wdfs.delete(new Path(od, "G.dat"), true);

            FileUtil.copyMerge(wdfs, new Path(wd, "G"), odfs, new Path(od, "G.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(od, "G.txt", "G.dat");

            // Clear working directory
            if (wdfs.exists(wd)) {
                wdfs.delete(wd, true);
            }
            wdfs.mkdirs(wd);

            // A = X.dot(G)
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", "G.dat");
            configuration.set("prefix", "a:");
            new MM2(configuration, inputFile, workingDirectory + "/A").run();

            // B = G.T.dot(G)
            new MM3(configuration, outputDirectory + "/G.txt", workingDirectory + "/mm3").run();

            // B = F.dot(B)
            configuration.set("mpath", workingDirectory + "/mm3");
            configuration.setInt("mw", k);
            configuration.setInt("mh", k);
            configuration.set("prefix", "b:");
            new MM1(configuration, outputDirectory + "/F.txt", workingDirectory + "/B").run();

            // Update F
            new MatrixUpdater(
                    configuration,
                    new String[]{
                            workingDirectory + "/A",
                            workingDirectory + "/B",
                            outputDirectory + "/F.txt"
                    },
                    workingDirectory + "/F"
            ).run();

            wdfs.delete(new Path(od, "F.txt"), true);
            wdfs.delete(new Path(od, "F.dat"), true);
            FileUtil.copyMerge(wdfs, new Path(wd, "F"), odfs, new Path(od, "F.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(od, "F.txt", "F.dat");

            // Measure distance
            configuration.set("mpath", workingDirectory + "/GT");
            configuration.setInt("mw", k);
            configuration.setInt("mh", m);
            new Transposer(configuration, outputDirectory + "/G.txt", workingDirectory + "/GT").run();
            configuration.setInt("mw", m);
            configuration.setInt("mh", k);
            configuration.unset("prefix");
            new MM1(configuration, outputDirectory + "/F.txt", workingDirectory + "/X").run();

            configuration.setInt("mw", m);
            Double result = new DistanceFinder(configuration, inputFile, workingDirectory + "/X", "dist", "tmp.txt").run();

            resultHolder.appendResult(result);

        }

    }
}
