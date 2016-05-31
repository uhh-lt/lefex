package de.tudarmstadt.lt.wsi;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import java.io.File;

import static org.junit.Assert.*;


public class ExtractLexicalSamplesFeaturesTest {
    /*
    * The test is used as an example of class usage without any checks.
    * */

    private void run(String inputPath) throws Exception {
        // Initialization
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    private void run(String inputPath, Configuration conf) throws Exception {
        // Initialization
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        ToolRunner.run(conf, new ExractLexicalSampleFeatures(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexsample-20.csv").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath);
    }

    @Test
    public void testPRJ() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexsample-prj60.csv").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath);
    }

    @Test
    public void testTrigramPRJ() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexsample-prj60.csv").getFile());
        String inputPath = file.getAbsolutePath();

        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "trigram");
            run(inputPath, conf);
    }

    @Test
    public void testDependencyPRJ() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexsample-prj60.csv").getFile());
        String inputPath = file.getAbsolutePath();

        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "dependency");
        run(inputPath, conf);
    }

    @Test
    public void testDependencyAndTrigramPRJ() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/lexsample-prj60.csv").getFile());
        String inputPath = file.getAbsolutePath();

        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "dependency+trigram");
        run(inputPath, conf);
    }

    // /Users/alex/work/joint/eval/contextualization-eval/data is https://github.com/tudarmstadt-lt/context-eval/tree/master/data

    @Test
    public void testOnSemEval() throws Exception {
        String inputPath = "/Users/alex/work/joint/eval/contextualization-eval/data/Dataset-SemEval-2013-13.csv";
        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "dependency+trigram");
        run(inputPath, conf);
    }

    @Test
    public void testOnTWSI() throws Exception {
        String inputPath = "/Users/alex/work/joint/eval/contextualization-eval/data/Dataset-TWSI-2.csv";
        Configuration conf = new Configuration();
        conf.setStrings("holing.type", "dependency+trigram");
        run(inputPath, conf);
    }
}

