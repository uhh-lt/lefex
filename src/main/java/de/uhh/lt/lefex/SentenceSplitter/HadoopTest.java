package de.uhh.lt.lefex.SentenceSplitter;

import de.uhh.lt.lefex.Utils.Format;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import java.io.File;
import java.util.List;
import static org.junit.Assert.*;
import org.jsoup.Jsoup;


public class HadoopTest {

    @Test
    public void jsoupTest() {
        String inputHtml = "<h1>Website Usability Tips, Tricks and Mistakes. Roundup from DesignFloat</h1>";
        String outputNoHtml = "Website Usability Tips, Tricks and Mistakes. Roundup from DesignFloat";
        assertEquals(outputNoHtml, Jsoup.parse(inputHtml).text());
    }

    @Test
    public void textWithHtmlTags() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test/text-with-tags.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath, inputPath + "-out", true, 61, true);
    }

    @Test
    public void textWithHtmlTagsLarge() throws Exception {
        String inputPath = "/Users/alex/Desktop/Lic_publicdomain_Lang_en_NoBoilerplate_true_MinHtml_true-r-00017.seg-00000.warc.gz";
        run(inputPath, inputPath + "-out", true, 61, true);
    }

    @Test
    public void testDefault() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test/ukwac-sample-10-oneliner.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath, inputPath + "-out", false, 14);
    }

    @Test
    public void testUniq() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test/ukwac-sample-10-oneliner.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath, inputPath + "-out", true, 10);
    }


    private void run(String inputPath, String outputDir, boolean makeUniq, int expectedLinesNum) throws Exception {
        run(inputPath, outputDir, makeUniq, expectedLinesNum, false);
    }

    private void run(String inputPath, String outputDir, boolean makeUniq, int expectedLinesNum, boolean stripHtml) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("strip_html", stripHtml);
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