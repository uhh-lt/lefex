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
    public void testMain() throws Exception {
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
        conf.setBoolean("holing.dependencies", true);
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);

        ToolRunner.run(conf, new JoBimExtractAndCount(), new String[]{inputPath, outputDir});

        // Parse the output and check the output data
        String depWFPath = (new File(outputDir, "DepWF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(depWFPath), Charset.forName("UTF-8"));
        assertTrue(lines.size() == 704);
        System.out.println("Number of lines int the file:" + lines.size());
        System.out.println("True:" + (lines.size() == 704));

        Set<String> expectedDeps = new HashSet<>(Arrays.asList("punct(@,date)", "prep_at(list,@)", "det(@,headstock)"));

        for(String line : lines) {
           String[] fields = line.split("\t");
           String dep = fields.length == 3 ? fields[1] : "";
           if (expectedDeps.contains(dep)) expectedDeps.remove(dep);
        }

        assertTrue(expectedDeps.size() == 0); // all expected deps are found
    }
}