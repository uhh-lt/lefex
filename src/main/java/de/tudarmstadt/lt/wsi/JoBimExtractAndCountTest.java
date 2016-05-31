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


public class JoBimExtractAndCountTest {

    @Test
    public void testDependencyHoling() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "dependency");
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});

        // Parse the output and check the output data
        String WFPath = (new File(outputDir, "WF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
        assertTrue("Number of lines is wrong.", lines.size() == 704);

        Set<String> expectedDeps = new HashSet<>(Arrays.asList("punct(@,date)", "prep_at(list,@)", "det(@,headstock)"));
        for(String line : lines) {
           String[] fields = line.split("\t");
           String dep = fields.length == 3 ? fields[1] : "";
           if (expectedDeps.contains(dep)) expectedDeps.remove(dep);
        }

        assertTrue("Some features are missing in the file.", expectedDeps.size() == 0); // all expected deps are found
    }

    @Test
    public void testTrigramHolingPRJ() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/python-ruby-jaguar.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);


        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);
        conf.setInt("holing.processeach", 1);
        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});
    }

    @Test
    public void testTrigramWithCoocsBig() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        String inputPath = "/Users/sasha/Desktop/debug/h1";
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", true);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);
        conf.setInt("holing.processeach", 1);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});
    }

    @Test
    public void testTrigramWithCoocsBigEachTenth() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        String inputPath = "/Users/sasha/Desktop/debug/h1";
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", true);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);
        conf.setInt("holing.processeach", 10);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});
    }

    @Test
    public void testTrigramWithCoocs() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", true);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});

        // Parse the output and check the output data
        String WFPath = (new File(outputDir, "WF-r-00000")).getAbsolutePath();

        // Check existance of the co-occurrence files

        // Check that the co-occurrence file contains a hidden co-occurrence pair

//        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
//        assertTrue("Number of lines is wrong.", lines.size() == 404);
//
//        Set<String> expectedFeatures = new HashSet<>(Arrays.asList("place_@_#","#_@_yet", "sum_@_a", "give_@_of"));
//        for(String line : lines) {
//            String[] fields = line.split("\t");
//            String feature = fields.length == 3 ? fields[1] : "";
//            if (expectedFeatures.contains(feature)) expectedFeatures.remove(feature);
//        }
//
//        assertTrue("Some features are missing in the file.", expectedFeatures.size() == 0); // all expected features are found
    }

    @Test
    public void testTrigram() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});

        // Parse the output and check the output data
        String WFPath = (new File(outputDir, "WF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
        assertTrue("Number of lines is wrong.", lines.size() == 404);

        Set<String> expectedFeatures = new HashSet<>(Arrays.asList("place_@_#","#_@_yet", "sum_@_a", "give_@_of"));
        for(String line : lines) {
            String[] fields = line.split("\t");
            String feature = fields.length == 3 ? fields[1] : "";
            if (expectedFeatures.contains(feature)) expectedFeatures.remove(feature);
        }

        assertTrue("Some features are missing in the file.", expectedFeatures.size() == 0); // all expected features are found
    }

    @Test
    public void tesTrigramNoLemmatization() throws Exception {
        // Initialization
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        // Action
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);
        conf.setBoolean("holing.lemmatize", false);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});

        // Parse the output and check the output data
        String WFPath = (new File(outputDir, "WF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
        assertTrue("Number of lines is wrong.", lines.size() == 404);

        Set<String> expectedFeatures = new HashSet<>(Arrays.asList("was_@_very","#_@_yet", "sum_@_a", "gave_@_of", "other_@_products"));
        for(String line : lines) {
            String[] fields = line.split("\t");
            String feature = fields.length == 3 ? fields[1] : "";
            if (expectedFeatures.contains(feature)) expectedFeatures.remove(feature);
        }

        assertTrue("Some features are missing in the file.", expectedFeatures.size() == 0); // all expected features are found
    }
}