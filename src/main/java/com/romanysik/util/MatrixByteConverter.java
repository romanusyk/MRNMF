package com.romanysik.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 *
 * Converts dense matrix .txt to .dat matrix and vice versa
 *
 * Created by romm on 03.04.17.
 */
public class MatrixByteConverter {

    public static void txt2dat(Path dir, String inputFile, String outputFile)
            throws IOException {

        FileSystem fileSystem = dir.getFileSystem(new Configuration());

        Path in = new Path(dir, inputFile);
        Path out = new Path(dir, outputFile);

        FSDataInputStream fsDataInputStream = fileSystem.open(in);
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        FSDataOutputStream writer = fileSystem.create(out);

        try {
            String line;
            line = reader.readLine();
            while (line != null){

                String[] keyVal = line.split("\\t");
                writer.writeLong(Long.parseLong(keyVal[0]));

                for (String aij : keyVal[1].split(",")) {
                    writer.writeDouble(Double.parseDouble(aij));
                }

                line = reader.readLine();
            }
        } finally {
            reader.close();
            inputStreamReader.close();
            fsDataInputStream.close();
            writer.flush();
            writer.close();
        }
    }

    public static void dat2txt(Path dir, String inputFile, String outputFile, int n, int m)
            throws IOException {

        FileSystem fileSystem = dir.getFileSystem(new Configuration());

        Path in = new Path(dir, inputFile);
        Path out = new Path(dir, outputFile);

        FSDataInputStream reader = fileSystem.open(in);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(out);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
        BufferedWriter writer = new BufferedWriter(outputStreamWriter);

        for (int i = 0; i < n; i++) {
            writer.write(reader.readLong() + "\t");
            for (int j = 0; j < m; j++) {
                writer.write("" + reader.readDouble());
                if (j < m - 1) {
                    writer.write(",");
                }
            }
            if (i < n - 1) {
                writer.write("\n");
            }
        }

        reader.close();
        writer.flush();
        writer.close();
        outputStreamWriter.close();
        fsDataOutputStream.close();

    }

}
