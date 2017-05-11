package com.romanysik.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by romm on 11.05.17.
 */
public class ResultHolder {

    private List<Long> time;
    private List<Double> result;

    private long timestamp;

    private FileSystem fs;
    private Path file;

    public ResultHolder(FileSystem fs, Path file) {

        this.fs = fs;
        this.file = file;

        time = new LinkedList<>();
        result = new LinkedList<>();

        tick();

    }

    public void tick() {
        timestamp = System.currentTimeMillis();
    }

    public void save() throws IOException {
        if (fs.exists(file)) {
            fs.delete(file, false);
        }
        FSDataOutputStream fsDataOutputStream = fs.create(file);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
        BufferedWriter writer = new BufferedWriter(outputStreamWriter);

        for (int i = 0; i < time.size(); i++) {
            writer.write("" + i + "," + time.get(i) + "," + result.get(i) + "\n");
        }

        writer.flush();
        writer.close();
        outputStreamWriter.close();
        fsDataOutputStream.close();

    }

    public void appendResult(Double newResult) throws IOException {
        long newTime = System.currentTimeMillis() - timestamp;
        time.add(newTime);
        result.add(newResult);
        save();
    }
}
