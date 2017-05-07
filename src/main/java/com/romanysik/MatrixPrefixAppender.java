package com.romanysik;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by romm on 07.05.17.
 */
public class MatrixPrefixAppender {

    public static void appendPrefix(Path dir, String prefix, String inputFile) throws IOException {
        appendPrefix(dir, prefix, inputFile, prefix + "_" + inputFile);
    }

    public static void appendPrefix(Path dir, String prefix, String inputFile, String outputFile)
            throws IOException {

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
            while ((line = reader.readLine()) != null){

                String[] keyVal = line.split("\\t");
                writer.write(keyVal[0] + "\t" + prefix + keyVal[1] + "\n");

            }

        } finally {
            reader.close();
            inputStreamReader.close();
            fsDataInputStream.close();
            writer.flush();
            writer.close();
        }
    }

}
