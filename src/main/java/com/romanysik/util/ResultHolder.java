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
 * Holds learning results
 *
 * De-facto stores two arrays:
 *      time - cumulative time of each iteration
 *      result - score of each iteration
 *
 * Created by romm on 11.05.17.
 */
public class ResultHolder {

    private List<Long> time;
    private List<Double> result;

    private long timestamp;

    private FileSystem fs;
    private Path file;

    /**
     * Initialize arrays and makes first tick
     *
     * @param fs - FileSystem
     * @param file - and Path to put report.txt
     */
    public ResultHolder(FileSystem fs, Path file) {

        this.fs = fs;
        this.file = file;

        time = new LinkedList<>();
        result = new LinkedList<>();

        tick();

    }

    /**
     * Save time when learning began
     * This method is called by constructor, by can be called directly if need
     */
    public void tick() {
        timestamp = System.currentTimeMillis();
    }

    /**
     * Saves result to report.txt according to the format
     *
     * <iteration>,<time>,<score>
     *
     * @throws IOException
     */
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

    /**
     * Appends score, time and saves current result to report.txt
     *
     * @param newResult
     * @throws IOException
     */
    public void appendResult(Double newResult) throws IOException {
        long newTime = System.currentTimeMillis() - timestamp;
        time.add(newTime);
        result.add(newResult);
        save();
    }
}
