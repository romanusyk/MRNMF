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
public class ConvexNMF implements Algorithm {

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

    public ConvexNMF(String inputFile, int n, int m, int k, int r,
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

        // Init G matrix randomly
        bufferedFileWriter.open(odfs, new Path(od, "G.txt"));
        MatrixInitializer.writeRandomMatrix(m, k, r, bufferedFileWriter);
        bufferedFileWriter.close();

        // Init W matrix randomly
        bufferedFileWriter.open(odfs, new Path(od, "W.txt"));
        MatrixInitializer.writeRandomMatrix(m, k, r, bufferedFileWriter);
        bufferedFileWriter.close();

        MatrixByteConverter.txt2dat(od, "G.txt", "G.dat");
        MatrixByteConverter.txt2dat(od, "W.txt", "W.dat");

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

            configuration.unset("datLoc");

            // XW = X.dot(W)
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", "W.dat");
            configuration.setInt("k", k);
            new MM2(configuration, inputFile, workingDirectory + "/XW").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "XW"), wdfs, new Path(wd, "XW.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(wd, "XW.txt", "XW.dat");

            // XTXW = X.T.dot(XW)
            configuration.setBoolean("transposeX", true);
            configuration.set("mpath", "XW.dat");
            configuration.setInt("k", k);
            configuration.set("datLoc", "wd");
//            configuration.set("prefix", "n:");
            new MM2(configuration, inputFile, workingDirectory + "/XTXW").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "XTXW"), wdfs, new Path(wd, "XTXW.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(wd, "XTXW.txt", "XTXW.dat");

            // W to W_sparse
            SparseDenseMatrixConverter.dense2sparse(od, "W.txt", "W_sparse.txt");

            // WTXTXW = W.T.dot(X.T.dot(XW))
            configuration.setBoolean("transposeX", true);
            configuration.set("mpath", "XTXW.dat");
            configuration.setInt("k", k);
            configuration.set("datLoc", "wd");
//            configuration.set("prefix", "n:");
            new MM2(configuration, outputDirectory + "/W_sparse.txt", workingDirectory + "/WTXTXW").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "WTXTXW"), wdfs, new Path(wd, "WTXTXW.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(wd, "WTXTXW.txt", "WTXTXW.dat");

            // G to G_sparse
            SparseDenseMatrixConverter.dense2sparse(od, "G.txt", "G_sparse.txt");

            // GWTXTXW = G.dot(W.T.dot(X.T.dot(XW)))
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", "WTXTXW.dat");
            configuration.setInt("k", k);
            configuration.set("datLoc", "wd");
//            configuration.set("prefix", "n:");
            new MM2(configuration, outputDirectory + "/G_sparse.txt", workingDirectory + "/GWTXTXW").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "GWTXTXW"), wdfs, new Path(wd, "GWTXTXW.txt"), false, new Configuration(), "");

            configuration.unset("datLoc");

            // XG = X.dot(G)
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", "G.dat");
            configuration.setInt("k", k);
            new MM2(configuration, inputFile, workingDirectory + "/XG").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "XG"), wdfs, new Path(wd, "XG.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(wd, "XG.txt", "XG.dat");

            // XTXG = X.T.dot(XG)
            configuration.setBoolean("transposeX", true);
            configuration.set("mpath", "XG.dat");
            configuration.setInt("k", k);
            configuration.set("datLoc", "wd");
//            configuration.set("prefix", "n:");
            new MM2(configuration, inputFile, workingDirectory + "/XTXG").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "XTXG"), wdfs, new Path(wd, "XTXG.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(wd, "XTXG.txt", "XTXG.dat");

            // GTG = G.T.dot(G)
            new MM3(configuration, outputDirectory + "/G.txt", workingDirectory + "/GTG").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "GTG"), wdfs, new Path(wd, "GTG.txt"), false, new Configuration(), "");

            // XTXWGTG = X.T.dot(XW).dot(G.T.dot(G))
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", workingDirectory + "/GTG.txt");
            configuration.setInt("mw", k);
            configuration.setInt("mh", k);
//            configuration.set("prefix", "n:");
            new MM1(configuration, workingDirectory + "/XTXW.txt", workingDirectory + "/XTXWGTG").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "XTXWGTG"), wdfs, new Path(wd, "XTXWGTG.txt"), false, new Configuration(), "");

            //
            // Updating
            //

            MatrixPrefixAppender.appendPrefix(wd, "a:", "XTXW.txt", "numeratorW.txt");
            MatrixPrefixAppender.appendPrefix(wd, "b:", "GWTXTXW.txt", "denominatorW.txt");

            // Update W
            new MatrixUpdater(
                    configuration,
                    new String[]{
                            workingDirectory + "/numeratorW.txt",
                            workingDirectory + "/denominatorW.txt",
                            outputDirectory + "/W.txt"
                    },
                    workingDirectory + "/W",
                    true
            ).run();

            wdfs.delete(new Path(od, "W.txt"), true);
            wdfs.delete(new Path(od, "W.dat"), true);
            FileUtil.copyMerge(wdfs, new Path(wd, "W"), odfs, new Path(od, "W.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(od, "W.txt", "W.dat");

            MatrixPrefixAppender.appendPrefix(wd, "a:", "XTXG.txt", "numeratorG.txt");
            MatrixPrefixAppender.appendPrefix(wd, "b:", "XTXWGTG.txt", "denominatorG.txt");

            // Update G
            new MatrixUpdater(
                    configuration,
                    new String[]{
                            workingDirectory + "/numeratorG.txt",
                            workingDirectory + "/denominatorG.txt",
                            outputDirectory + "/G.txt"
                    },
                    workingDirectory + "/G",
                    true
            ).run();

            wdfs.delete(new Path(od, "G.txt"), true);
            wdfs.delete(new Path(od, "G.dat"), true);
            FileUtil.copyMerge(wdfs, new Path(wd, "G"), odfs, new Path(od, "G.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(od, "G.txt", "G.dat");

            //
            // Measure distance
            //
            configuration.unset("datLoc");

            // XW = X.dot(W)
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", "W.dat");
            configuration.setInt("k", k);
            new MM2(configuration, inputFile, workingDirectory + "/X1").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "X1"), wdfs, new Path(wd, "X1.txt"), false, new Configuration(), "");
            SparseDenseMatrixConverter.dense2sparse(wd, "X1.txt", "X1_sparse.txt");

            // Transpose G
            configuration.set("mpath", workingDirectory + "/GT");
            configuration.setInt("mh", m);
            configuration.setInt("mw", k);
            new Transposer(configuration, outputDirectory + "/G.txt", workingDirectory + "/GT").run();
            FileUtil.copyMerge(wdfs, new Path(wd, "GT"), wdfs, new Path(wd, "GT.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(wd, "GT.txt", "GT.dat");

            // XWGT = X.dot(W).dot(G.T)
            configuration.setBoolean("transposeX", false);
            configuration.set("datLoc", "wd");
            configuration.set("mpath", "GT.dat");
            configuration.setInt("k", m);
            new MM2(configuration, workingDirectory + "/X1_sparse.txt", workingDirectory + "/X2").run();

            configuration.setInt("mw", m);
            Double result = new DistanceFinder(configuration, inputFile, workingDirectory + "/X2", "dist", "tmp.txt").run();

            resultHolder.appendResult(result);

        }

    }
}
