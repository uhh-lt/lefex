package de.tudarmstadt.lt.jst.FixLineLength;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;


public class HadoopTest {

    /*
    @HadoopTest
    public void testOnBigText() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File("/Users/alex/work/joint/corpora/wiki-1m.txt");
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new FixLineLength(), new String[]{inputPath, outputDir});
    }*/

    @Test
    public void testRunJob() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10-oneliner.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new HadoopMain(), new String[]{inputPath, outputDir});

        // Parse the output and check the output data
        String outputPath = (new File(outputDir, "part-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(outputPath), Charset.forName("UTF-8"));
        assertTrue("Number of lines is wrong.", lines.size() == 10);
    }
}