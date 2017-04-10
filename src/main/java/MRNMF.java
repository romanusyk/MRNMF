import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by romm on 30.03.17.
 */
public class MRNMF {

    private static CLIOption[] opts = new CLIOption[] {
            new CLIOption("i", true, "path to the input matrix file; if not set random matrix with given sparsity -s will be generated", false),
            new CLIOption("s", true, "sparsity of random generated matrix", false),
            new CLIOption("o", true, "path to the output directory"),
            new CLIOption("t", true, "path to the temporary directory"),
            new CLIOption("n", true, "matrix row number"),
            new CLIOption("m", true, "matrix column number"),
            new CLIOption("it", true, "iterations number"),
            new CLIOption("k", true, "the dimension number to be reduced to"),
            new CLIOption("r", true, "the range value for the matrix elements"),
            new CLIOption("h", false, "print help for this application", false)
    };

    public static void usage() {
        System.out.print("Usage: hadoop jar MRNMF.jar nmf.MRNMF\n");
        for (CLIOption o : opts) {
            System.out.println(o);
        }
    }

    public static void main(String[] args) throws ParseException, IOException, InterruptedException, ClassNotFoundException {

        // parsing CLI optons
        Options options = new Options();
        for (CLIOption o : opts) {
            options.addOption(o.getName(), o.hasArg(), o.getDescription());
        }
        BasicParser parser = new BasicParser();
        CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption('h')) {
            usage();
            return;
        }

        for (CLIOption o : opts) {
            if (o.isRequired() && !commandLine.hasOption(o.getName())) {
                usage();
                return;
            }
        }

        String inputFile = commandLine.getOptionValue("i");
        double sparsity = Double.parseDouble(commandLine.getOptionValue("s", "0.5"));
        String outputDirectory = commandLine.getOptionValue("o");
        String workingDirectory = commandLine.getOptionValue("t");
        int n = Integer.parseInt(commandLine.getOptionValue("n"));
        int m = Integer.parseInt(commandLine.getOptionValue("m"));
        int it = Integer.parseInt(commandLine.getOptionValue("it"));
        int k = Integer.parseInt(commandLine.getOptionValue("k"));
        int r = Integer.parseInt(commandLine.getOptionValue("r"));

        // Creating output directory
        Path od = new Path(outputDirectory);
        FileSystem odfs = od.getFileSystem(new Configuration());
        if (odfs.exists(od)) {
            odfs.delete(od, true);
        }
        odfs.mkdirs(od);

        // Creating working directory
        Path wd = new Path(workingDirectory);
        FileSystem wdfs = wd.getFileSystem(new Configuration());
        odfs.mkdirs(new Path(od, "dist"));

        // Buffered writer for matrix initialization
        BufferedFileWriter bufferedFileWriter = new BufferedFileWriter();

        // Init input matrix randomly if not given
        if (inputFile == null) {
            bufferedFileWriter.open(wdfs, new Path("input.txt"));
            MatrixInitializer.writeRandomSparseMatrix(n, m, r, sparsity, bufferedFileWriter);
            bufferedFileWriter.close();
            inputFile = "input.txt";
        }

        // Init F matrix randomly
        bufferedFileWriter.open(odfs, new Path(od, "F.txt"));
        MatrixInitializer.writeRandomMatrix(n, k, r, bufferedFileWriter);
        bufferedFileWriter.close();

        // Init G matrix randomly
        bufferedFileWriter.open(odfs, new Path(od, "G.txt"));
        MatrixInitializer.writeRandomMatrix(m, k, r, bufferedFileWriter);
        bufferedFileWriter.close();

        MatrixByteConverter.txt2dat(od, "F.txt", "F.dat");
        MatrixByteConverter.txt2dat(od, "G.txt", "G.dat");

        Configuration configuration = new Configuration();

        configuration.set("wd", workingDirectory);
        configuration.set("od", outputDirectory);

        for (int i = 0; i < it; i++) {

            // Clear working directory
            if (wdfs.exists(wd)) {
                wdfs.delete(wd, true);
            }
            wdfs.mkdirs(wd);

            // A = X.T.dot(F)
            configuration.setBoolean("transposeX", true);
            configuration.set("mpath", "F.dat");
            configuration.setInt("k", k);
            configuration.set("prefix", "a:");
            new MM2(configuration, inputFile, workingDirectory + "/A").run();

            // B = F.T.dot(F)
            new MM3(configuration, outputDirectory + "/F.txt", workingDirectory + "/mm3").run();

            // B = G.dot(B)
            configuration.set("mpath", workingDirectory + "/mm3");
            configuration.setInt("mw", k);
            configuration.setInt("mh", k);
            configuration.set("prefix", "b:");
            new MM1(configuration, outputDirectory + "/G.txt", workingDirectory + "/B").run();

            // Update G
            new MatrixUpdater(
                    configuration,
                    new String[]{
                            workingDirectory + "/A",
                            workingDirectory + "/B",
                            outputDirectory + "/G.txt"
                    },
                    workingDirectory + "/G"
            ).run();

            wdfs.delete(new Path(od, "G.txt"), true);
            wdfs.delete(new Path(od, "G.dat"), true);

            FileUtil.copyMerge(wdfs, new Path(wd, "G"), odfs, new Path(od, "G.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(od, "G.txt", "G.dat");

            // Clear working directory
            if (wdfs.exists(wd)) {
                wdfs.delete(wd, true);
            }
            wdfs.mkdirs(wd);

            // A = X.dot(G)
            configuration.setBoolean("transposeX", false);
            configuration.set("mpath", "G.dat");
            configuration.set("prefix", "a:");
            new MM2(configuration, inputFile, workingDirectory + "/A").run();

            // B = G.T.dot(G)
            new MM3(configuration, outputDirectory + "/G.txt", workingDirectory + "/mm3").run();

            // B = F.dot(B)
            configuration.set("mpath", workingDirectory + "/mm3");
            configuration.setInt("mw", k);
            configuration.setInt("mh", k);
            configuration.set("prefix", "b:");
            new MM1(configuration, outputDirectory + "/F.txt", workingDirectory + "/B").run();

            // Update F
            new MatrixUpdater(
                    configuration,
                    new String[]{
                            workingDirectory + "/A",
                            workingDirectory + "/B",
                            outputDirectory + "/F.txt"
                    },
                    workingDirectory + "/F"
            ).run();

            wdfs.delete(new Path(od, "F.txt"), true);
            wdfs.delete(new Path(od, "F.dat"), true);
            FileUtil.copyMerge(wdfs, new Path(wd, "F"), odfs, new Path(od, "F.txt"), false, new Configuration(), "");
            MatrixByteConverter.txt2dat(od, "F.txt", "F.dat");

            // Measure distance
            configuration.set("mpath", workingDirectory + "/GT");
            configuration.setInt("mw", k);
            configuration.setInt("mh", m);
            new Transposer(configuration, outputDirectory + "/G.txt", workingDirectory + "/GT").run();
            configuration.setInt("mw", m);
            configuration.setInt("mh", k);
            configuration.unset("prefix");
            new MM1(configuration, outputDirectory + "/F.txt", workingDirectory + "/X").run();

            configuration.setInt("mw", m);
            new DistanceFinder(configuration, inputFile, workingDirectory + "/X", workingDirectory + "/dist").run();

            FileUtil.copyMerge(wdfs, new Path(wd, "dist"), odfs, new Path(od, "dist/" + i + ".txt"), false, new Configuration(), "");

        }

        FileUtil.copyMerge(odfs, new Path(od, "dist"), odfs, new Path(od, "dist.csv"), false, new Configuration(), "");

    }

    public static void printArray(Object[] array) {
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + "\t");
        }
        System.out.println();
    }

    public static void printMatrix(Object[][] array) {
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[0].length; j++) {
                System.out.print(array[i][j] + "\t");
            }
            System.out.println();
        }
    }

}
