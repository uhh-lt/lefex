package de.tudarmstadt.lt.wsi;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static org.junit.Assert.*;


public class ExtractLexicalSamplesFeaturesTest {
    @Test
    public void testDefaultConfiguration() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexical-sample-dataset-20.csv").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    @Test
    public void testTrigrams() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexical-sample-dataset-20.csv").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "trigram");
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
    }

    @Test
    public void testA() throws Exception {
        // Initialization
        String inputPath = "/Users/sasha/Desktop/debug/prj/dataset.csv";
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    @Test
    public void testOnSemEval() throws Exception {
        // Initialization
        String inputPath = "/Users/sasha/Desktop/debug/wsd/Dataset-SemEval-2013-13.csv";
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    @Test
    public void testTrigramsSemeval() throws Exception {
        // Initialization
        String inputPath = "/Users/sasha/Desktop/debug/wsd/Dataset-SemEval-2013-13.csv";
        String outputDir = inputPath + "-trigrams";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "trigram");
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
    }

    @Test
    public void testOnTWSI() throws Exception {
        // Initialization
        String inputPath = "/Users/sasha/Desktop/debug/wsd/Dataset-TWSI-2.csv";
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    @Test
    public void testTrigramsTWSI() throws Exception {
        // Initialization
        String inputPath = "/Users/sasha/Desktop/debug/wsd/Dataset-TWSI-2.csv";
        String outputDir = inputPath + "-trigrams";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "trigram");
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
    }

}

