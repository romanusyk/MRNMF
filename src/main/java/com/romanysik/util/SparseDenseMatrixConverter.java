package com.romanysik.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Converts dense matrix to sparse matrix and vice versa
 * Created by romm on 07.05.17.
 */
public class SparseDenseMatrixConverter {

    public static void dense2sparse(Path dir, String inputFile, String outputFile) throws IOException {

        FileSystem fileSystem = dir.getFileSystem(new Configuration());

        Path in = new Path(dir, inputFile);
        Path out = new Path(dir, outputFile);

        FSDataInputStream fsDataInputStream = fileSystem.open(in);
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(out);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
        BufferedWriter writer = new BufferedWriter(outputStreamWriter);

        try {
            String line;
            line = reader.readLine();
            while (line != null){

                String[] keyVal = line.split("\\t");
                String i = keyVal[0];

                int j = 1;
                for (String aij : keyVal[1].split(",")) {
                    writer.write(i + "\t" + j + "\t" + aij + "\n");
                    j++;
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

    public static void sparse2dense(Path dir, String inputFile, String outputFile) throws IOException {
        // TODO
    }

}
