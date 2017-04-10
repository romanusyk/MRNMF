import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Locale;
import java.util.Random;

/**
 * Created by romm on 03.04.17.
 */
public class MatrixInitializer {

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
