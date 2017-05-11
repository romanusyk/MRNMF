package com.romanysik.util;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;

/**
 *
 * Create random dense or sparse matrix
 *
 * Created by romm on 03.04.17.
 */
public class MatrixInitializer {

    /**
     * Creates random dense matrix of shape (n, m) with elements in given range
     * @param n
     * @param m
     * @param range
     * @param writer
     * @throws IOException
     */
    public static void writeRandomMatrix(long n, long m, double range, BufferedFileWriter writer) throws IOException {

        Random random = new Random();

        for (long i = 0; i < n; i++) {

            writer.get().write(i + 1 + "\t");

            for (int j = 0; j < m; j++) {

                double value = random.nextDouble() * range;

                writer.get().write(value + "");
                if (j < m - 1) {
                    writer.get().write(",");
                } else {
                    writer.get().write("\n");
                }

            }

            writer.get().flush();
        }

    }

    /**
     * Creates random sparse matrix of shape (n, m) with elements in given range
     * with given sparsity from [0, 1]
     * Element is non-zero if random.nextDouble() > sparsity
     *
     * @param n
     * @param m
     * @param range
     * @param sparsity
     * @param writer
     * @throws IOException
     */
    public static void writeRandomSparseMatrix(long n, long m, double range, double sparsity, BufferedFileWriter writer) throws IOException {

        Random random = new Random();

        for (long i = 0; i < n; i++) {

            for (long j = 0; j < m; j++) {

                if (random.nextDouble() > sparsity) {

                    double value = random.nextDouble() * range;
                    writer.get().write(String.format(Locale.US, "%d\t%d\t%f\n", i + 1, j + 1, value));

                }
            }
            writer.get().flush();
        }

    }

}
