package de.tudarmstadt.lt.jst.CoNLL;

import de.tudarmstadt.lt.jst.Utils.Format;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;


public class HadoopTest {
    private void run(String inputPath, Configuration conf, Integer expectedLines) throws Exception {
        // Initialization
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        // Action
        ToolRunner.run(conf, new HadoopMain(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);

        de.tudarmstadt.lt.jst.ExtractTermFeatureScores.HadoopTest.TestPaths paths = new de.tudarmstadt.lt.jst.ExtractTermFeatureScores.HadoopTest.TestPaths("");

        String WFPath = (new File(paths.getOutputDir(), "WF-r-00000.gz")).getAbsolutePath();
        List<String> lines = Format.readGzipAsList(WFPath);
        assertEquals("Number of lines in WF file is wrong.", expectedLengthWF, lines.size());

    }

    private void run(String inputPath) throws Exception {
        run(inputPath, new Configuration(), -1);
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath);
    }

    @Test
    public void testNoCollapsingDefault() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        Configuration conf = new Configuration();
        conf.setBoolean("collapsing", false);
        run(inputPath, conf);
    }

    @Test
    public void testNoCollapsingMalt() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        Configuration conf = new Configuration();
        conf.setBoolean("collapsing", false);
        conf.setStrings("parserName", "malt");
        run(inputPath, conf);
    }
}

