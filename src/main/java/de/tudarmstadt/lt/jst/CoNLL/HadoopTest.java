package de.tudarmstadt.lt.jst.CoNLL;

import de.tudarmstadt.lt.jst.TestPaths;
import de.tudarmstadt.lt.jst.Utils.Format;
import de.tudarmstadt.lt.jst.Utils.Resources;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.File;
import java.util.List;

import static org.junit.Assert.*;


public class HadoopTest {
    private void run(Configuration conf, long expectedLines) throws Exception {
        TestPaths paths = new TestPaths("standard");
        FileUtils.deleteDirectory(new File(paths.getOutputDir()));
        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir(), "false"});

        String outputPath = (new File(paths.getOutputDir(), "part-m-00000")).getAbsolutePath();
        List<String> lines = Format.readAsList(outputPath);
        assertEquals("Number of lines in the output file is wrong.", expectedLines, lines.size());
    }

    @Test
    public void documentInputFile() throws Exception {
        String inputPath = "/Users/sasha/work/active/joint/JoSimText/target/scala-2.11/test-classes/cc-test.warc.gz-output";
        String outputPath = "/Users/sasha/work/active/joint/JoSimText/target/scala-2.11/test-classes/cc-test.warc.gz-output-conll";
        FileUtils.deleteDirectory(new File(outputPath));

        Configuration conf = new Configuration();
        conf.setBoolean("collapsing", true);
        conf.setStrings("parserName", "malt");
        conf.setStrings("inputType", "document");
        ToolRunner.run(conf, new HadoopMain(), new String[]{inputPath, outputPath, "false"});
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        run(new Configuration(), 389);
    }


    @Test
    public void testNoCollapsingDefault() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("collapsing", false);
        run(conf, 419);
    }

    @Test
    public void testNoCollapsingMalt() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("collapsing", false);
        conf.setStrings("parserName", "malt");
        run(conf, 419);
    }

    @Test
    public void testNoCollapsingStanford() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("collapsing", false);
        conf.setStrings("parserName", "stanford");
        run(conf, 321);
    }

    @Test
    public void testTypeSpllitting(){
        String r = de.tudarmstadt.lt.jst.CoNLL.HadoopMap.getShortName(
                "de.tudarmstadt.ukp.dkpro.core.api.ner.type.Person");
        assertEquals(r, "Person");

        r = de.tudarmstadt.lt.jst.CoNLL.HadoopMap.getShortName(
                "de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity");
        assertEquals(r, "NamedEntity");

        r = de.tudarmstadt.lt.jst.CoNLL.HadoopMap.getShortName(
                "NamedEntity");
        assertEquals(r, "NamedEntity");

        r = de.tudarmstadt.lt.jst.CoNLL.HadoopMap.getShortName(
                "");
        assertEquals(r, "");

        r = de.tudarmstadt.lt.jst.CoNLL.HadoopMap.getShortName(
                "NamedEntityafkjaskljfaklsjfklasjf");
        assertEquals(r, "NamedEntityafkjaskljfaklsjfklasjf");
    }

}

