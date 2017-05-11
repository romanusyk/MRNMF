package com.romanysik.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by romm on 10.04.17.
 */
public class BufferedFileWriter {

    private FSDataOutputStream fsDataOutputStream;
    private OutputStreamWriter outputStreamWriter;
    private BufferedWriter writer;

    public void open(FileSystem fs, Path path) throws IOException {
        fsDataOutputStream = fs.create(path);
        outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
        writer = new BufferedWriter(outputStreamWriter);
    }

    public BufferedWriter get() {
        return writer;
    }

    public void close() throws IOException {
        writer.flush();
        writer.close();
        outputStreamWriter.close();
        fsDataOutputStream.close();
    }
}
