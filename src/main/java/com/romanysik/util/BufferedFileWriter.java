package com.romanysik.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 *
 * FSDataOutputStream wrapper
 *
 * Created by romm on 10.04.17.
 */
public class BufferedFileWriter {

    private FSDataOutputStream fsDataOutputStream;
    private OutputStreamWriter outputStreamWriter;
    private BufferedWriter writer;

    /**
     * Creates BufferedWriter to file with given
     *
     * @param fs - FileSystem
     * @param path - Path to file
     * @throws IOException
     */
    public void open(FileSystem fs, Path path) throws IOException {
        fsDataOutputStream = fs.create(path);
        outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
        writer = new BufferedWriter(outputStreamWriter);
    }

    /**
     *
     * @return BufferedWriter to file
     */
    public BufferedWriter get() {
        return writer;
    }

    /**
     * Closes output stream
     * Should be called after writing is finished
     *
     * @throws IOException
     */
    public void close() throws IOException {
        writer.flush();
        writer.close();
        outputStreamWriter.close();
        fsDataOutputStream.close();
    }
}
