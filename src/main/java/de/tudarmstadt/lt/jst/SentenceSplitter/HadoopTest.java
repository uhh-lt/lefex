package de.tudarmstadt.lt.jst.SentenceSplitter;

import de.tudarmstadt.lt.jst.Utils.Format;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import java.io.File;
import java.util.List;
import static org.junit.Assert.*;


public class HadoopTest {
    @Test
    public void testDefault() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10-oneliner.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath, inputPath + "-out", false, 14);
    }

    @Test
    public void testUniq() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10-oneliner.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath, inputPath + "-out", true, 10);
    }

    private void run(String inputPath, String outputDir, boolean makeUniq, int expectedLinesNum) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new HadoopMain(), new String[]{
                inputPath, outputDir, String.valueOf(makeUniq), "true"});
        String outputPath = (new File(outputDir, "part-r-00000.gz")).getAbsolutePath();
        List<String> lines = Format.readGzipAsList(outputPath);
        assertTrue("Number of lines is wrong.", lines.size() == expectedLinesNum);
    }

    @Test
    public void regexTest() {
        String i1 = "abc   abc               abc";
        String o1 = "abc abc abc";
        assertEquals(i1.replaceAll("\\s+", " "), o1);

        String i2 = "abc   \nabc               abc";
        String o2 = "abc abc abc";
        assertEquals(i2.replaceAll("\\s+", " "), o2);
    }
}