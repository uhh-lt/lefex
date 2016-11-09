package de.tudarmstadt.lt.jst.ExtractTermFeatureScores;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import de.tudarmstadt.lt.jst.Utils.Resources;

import static org.junit.Assert.*;

public class HadoopTest {

    private void runDependencyHoling(boolean selfFeatures, boolean mwe, boolean ner, int expectedLengthWF,
        HashMap<String, List<String>> expectedWFPairs, HashMap<String, List<String>> unexpectedWFPairs,
        String depParser) throws Exception
    {
        runDependencyHoling(selfFeatures, mwe, ner, expectedLengthWF, expectedWFPairs, unexpectedWFPairs, depParser,
                false, -1, new HashSet<>(), new HashSet<>());
    }

    private void runDependencyHoling(boolean selfFeatures, boolean mwe, boolean ner, int expectedLengthWF,
        HashMap<String, List<String>> expectedWFPairs, HashMap<String, List<String>> unexpectedWFPairs,
        String depParser, boolean outputPos, int expectedLengthW,
        HashSet<String> expectedWFreq, HashSet<String> unexpectedWFreq) throws Exception
    {
        TestPaths paths = new TestPaths().invoke();
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "dependency");
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);
        String mwePath = mwe ? Resources.getJarResourcePath("data/voc-sample.csv") : "";
        conf.setStrings("holing.mwe.vocabulary", mwePath);
        conf.setBoolean("holing.mwe.self_features", selfFeatures);
        conf.setBoolean("holing.mwe.ner", ner);
        conf.setStrings("holing.dependencies.parser", depParser);
        conf.setBoolean("holing.output_pos", outputPos);

        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir()});

        String WFPath = (new File(paths.getOutputDir(), "WF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
        assertEquals("Number of lines in WF file is wrong.", expectedLengthWF, lines.size());

        for(String line : lines) {
            String[] fields = line.split("\t");
            String word = fields.length == 3 ? fields[0] : "";
            String feature = fields.length == 3 ? fields[1] : "";
            if (expectedWFPairs.containsKey(word) && expectedWFPairs.get(word).contains(feature)) {
                expectedWFPairs.get(word).remove(feature);
                if (expectedWFPairs.get(word).size() == 0) expectedWFPairs.remove(word);
            }
            if (unexpectedWFPairs.containsKey(word) && unexpectedWFPairs.get(word).contains(feature)) {
                fail("Unexpected feature in the WF file: " + word + "#" + feature);
            }
        }
        assertEquals("Some expected features are missing in the WF file.", 0, expectedWFPairs.size());

        // Chech for presntes of part-os-speech tags
        String wPath = (new File(paths.getOutputDir(), "W-r-00000")).getAbsolutePath();
        List<String> wLines = Files.readAllLines(Paths.get(wPath), Charset.forName("UTF-8"));
        if (expectedLengthW != -1) {
            assertEquals("Number of lines in W file is wrong.", expectedLengthW, wLines.size());
        }

        // Check expected and unexpected W
        for (String ew : expectedWFreq){
            assertTrue("Expected word is not present: " + ew, wLines.contains(ew));
        }

        for (String uw : unexpectedWFreq){
            assertFalse("Unexpected word is present: " + uw, wLines.contains(uw));
        }
    }

    @Test
    public void testDependencyHolingMweSelfFeaturesNER() throws Exception {
        HashMap<String, List<String>> expectedWF = new HashMap<>();
        expectedWF.put("Green pears", new LinkedList<>(Arrays.asList("nn(@,pear)","nn(Green,@)","subj(@,grow)")));
        expectedWF.put("very", new LinkedList<>(Arrays.asList("advmod(@,rigid)")));
        expectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,Road)","nn(@,park)","nn(Knoll,@)","prep_along(proceed,@)","prep_on(continue,@)")));
        expectedWF.put("rarely", new LinkedList<>(Arrays.asList("advmod(@,fit)")));

        HashMap<String, List<String>> unexpectedWF = new HashMap<>();
        unexpectedWF.put("rarely", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("very", new LinkedList<>(Arrays.asList("nn(@,the)")));

        runDependencyHoling(true, true, true, 770, expectedWF, unexpectedWF, "malt");
    }

    @Test
    public void testDependencyHolingMweSelfFeatures() throws Exception {
        HashMap<String, List<String>> expectedWF = new HashMap<>();
        expectedWF.put("Green pears", new LinkedList<>(Arrays.asList("nn(@,pear)","nn(Green,@)","subj(@,grow)")));
        expectedWF.put("very", new LinkedList<>(Arrays.asList("advmod(@,rigid)")));
        expectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,Road)","nn(@,park)","nn(Knoll,@)","prep_along(proceed,@)","prep_on(continue,@)")));
        expectedWF.put("rarely", new LinkedList<>(Arrays.asList("advmod(@,fit)")));

        HashMap<String, List<String>> unexpectedWF = new HashMap<>();
        unexpectedWF.put("rarely", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("very", new LinkedList<>(Arrays.asList("nn(@,the)")));

        runDependencyHoling(true, true, false, 752, expectedWF, unexpectedWF, "malt");
    }

    private void runDependencyHolingMweNoSelfFeaturesMalt(boolean outputPos) throws Exception {
        HashMap<String, List<String>> expectedWF = new HashMap<>();
        expectedWF.put("rarely", new LinkedList<>(Arrays.asList("advmod(@,fit)")));
        expectedWF.put("very", new LinkedList<>(Arrays.asList("advmod(@,rigid)")));
        expectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,park)","prep_along(proceed,@)","prep_on(continue,@)")));
        expectedWF.put("Green pears", new LinkedList<>(Arrays.asList("subj(@,grow)")));

        HashMap<String, List<String>> unexpectedWF = new HashMap<>();
        unexpectedWF.put("rarely", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("very", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,Road)","nn(Knoll,@)")));
        unexpectedWF.put("Green pears", new LinkedList<>(Arrays.asList("nn(@,pear)","nn(Green,@)")));

        HashSet<String> withPos = new HashSet<>();
        withPos.add("the#DT\t19");
        withPos.add("then#RB\t1");
        withPos.add("there#EX\t3");
        withPos.add("this#DT\t3");
        withPos.add("though#IN\t2");
        withPos.add("through#IN\t1");
        withPos.add("time#NN\t1");
        withPos.add("to#TO\t6");
        withPos.add("traffic#NN\t1");

        HashSet<String> withoutPos = new HashSet<>();
        withoutPos.add("the\t19");
        withoutPos.add("there\t3");
        withoutPos.add("this\t3");
        withoutPos.add("though\t2");
        withoutPos.add("through\t1");
        withoutPos.add("time\t1");
        withoutPos.add("to\t6");

        if (outputPos) {
            runDependencyHoling(false, true, false, 741, expectedWF, unexpectedWF, "malt", outputPos, 254, withPos, withoutPos);
        } else {
            runDependencyHoling(false, true, false, 741, expectedWF, unexpectedWF, "malt", outputPos, 241, withoutPos, withPos);
        }
    }

    @Test
    public void testDependencyHolingMweNoSelfFeaturesMaltNoPos() throws Exception {
        runDependencyHolingMweNoSelfFeaturesMalt(false);
    }

    @Test
    public void testDependencyHolingMweNoSelfFeaturesMaltPos() throws Exception {
        runDependencyHolingMweNoSelfFeaturesMalt(true);
    }

    @Test
    public void testDependencyHolingMweNoSelfFeaturesStanford() throws Exception {
        HashMap<String, List<String>> expectedWF = new HashMap<>();
        expectedWF.put("rarely", new LinkedList<>(Arrays.asList("advmod(@,fit)")));
        expectedWF.put("very", new LinkedList<>(Arrays.asList("advmod(@,rigid)")));
        expectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,park)","prep_along(proceed,@)","prep_on(continue,@)")));
        expectedWF.put("Green pears", new LinkedList<>(Arrays.asList("subj(@,grow)")));

        HashMap<String, List<String>> unexpectedWF = new HashMap<>();
        unexpectedWF.put("rarely", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("very", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,Road)","nn(Knoll,@)")));
        unexpectedWF.put("Green pears", new LinkedList<>(Arrays.asList("nn(@,pear)","nn(Green,@)")));

        runDependencyHoling(false, true, false, 619, expectedWF, unexpectedWF, "stanford");
    }

    @Test
    public void testDependencyHolingNoMWE() throws Exception {
        HashMap<String, List<String>> expectedWF = new HashMap<>();
        expectedWF.put("very", new LinkedList<>(Arrays.asList("advmod(@,rigid)")));
        expectedWF.put("rarely", new LinkedList<>(Arrays.asList("advmod(@,fit)")));

        HashMap<String, List<String>> unexpectedWF = new HashMap<>();
        unexpectedWF.put("rarely", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("very", new LinkedList<>(Arrays.asList("nn(@,the)")));
        unexpectedWF.put("Green pears", new LinkedList<>(Arrays.asList("nn(@,pear)","nn(Green,@)","subj(@,grow)")));
        unexpectedWF.put("Knoll Road", new LinkedList<>(Arrays.asList("nn(@,Road)","nn(@,park)","nn(Knoll,@)","prep_along(proceed,@)","prep_on(continue,@)")));

        runDependencyHoling(false, false, false, 726, expectedWF, unexpectedWF, "malt");
    }

    @Test
    public void testTrigramHolingLemmaPRJ() throws Exception {
        runTrigram(true);
    }

    @Test
    public void testTrigramHolingNoLemmaPRJ() throws Exception {
        runTrigram(false);
    }

    private void runTrigram(boolean lemmatize) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/python-ruby-jaguar.txt").getFile());
        String inputPath = file.getAbsolutePath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);
        conf.setInt("holing.processeach", 1);
        conf.setBoolean("holing.lemmatize", lemmatize);
        ToolRunner.run(conf, new HadoopMain(), new String[]{inputPath, outputDir});
    }

    @Test
    public void testTrigramWithCoocsBig() throws Exception {
        TestPaths paths = new TestPaths().invoke();
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", true);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);
        conf.setInt("holing.processeach", 1);

        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir()});
    }

    @Test
    public void testTrigramWithCoocsBigEachTenth() throws Exception {
        String inputPath = getTestCorpusPath();
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input text: " + inputPath);
        System.out.println("Output directory: "+  outputDir);

        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", true);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);
        conf.setInt("holing.processeach", 10);

        ToolRunner.run(conf, new HadoopMain(), new String[]{inputPath, outputDir});
    }

    private String getTestCorpusPath() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        return file.getAbsolutePath();
    }

    @Test
    public void testTrigramWithCoocs() throws Exception {
        TestPaths paths = new TestPaths().invoke();
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", true);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.nouns_only", false);

        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir()});
    }

    @Test
    public void testTrigram() throws Exception {
        TestPaths paths = new TestPaths().invoke();
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);

        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir()});

        String WFPath = (new File(paths.getOutputDir(), "WF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
        assertEquals("Number of lines is wrong.", 412, lines.size());

        Set<String> expectedFeatures = new HashSet<>(Arrays.asList("place_@_#","#_@_yet", "sum_@_a", "give_@_of"));
        for(String line : lines) {
            String[] fields = line.split("\t");
            String feature = fields.length == 3 ? fields[1] : "";
            if (expectedFeatures.contains(feature)) expectedFeatures.remove(feature);
        }
        assertTrue("Some features are missing in the file.", expectedFeatures.size() == 0); // all expected features are found
    }

    @Test
    public void testTrigramNoLemmatization() throws Exception {
        TestPaths paths = new TestPaths().invoke();
        Configuration conf = new Configuration();
        conf.setBoolean("holing.coocs", false);
        conf.setInt("holing.sentences.maxlength", 100);
        conf.setStrings("holing.type", "trigram");
        conf.setBoolean("holing.dependencies.semantify", true);
        conf.setBoolean("holing.nouns_only", false);
        conf.setBoolean("holing.dependencies.noun_noun_dependencies_only", false);
        conf.setBoolean("holing.lemmatize", false);

        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir()});

        String WFPath = (new File(paths.getOutputDir(), "WF-r-00000")).getAbsolutePath();
        List<String> lines = Files.readAllLines(Paths.get(WFPath), Charset.forName("UTF-8"));
        assertEquals("Number of lines is wrong.", 412, lines.size());

        Set<String> expectedFeatures = new HashSet<>(Arrays.asList("was_@_very","#_@_yet", "sum_@_a", "gave_@_of", "other_@_products"));
        for(String line : lines) {
            String[] fields = line.split("\t");
            String feature = fields.length == 3 ? fields[1] : "";
            if (expectedFeatures.contains(feature)) expectedFeatures.remove(feature);
        }
        assertTrue("Some features are missing in the file.", expectedFeatures.size() == 0); // all expected features are found
    }

    private class TestPaths {
        private String inputPath;
        private String outputDir;

        public String getInputPath() {
            return inputPath;
        }

        public String getOutputDir() {
            return outputDir;
        }

        public TestPaths invoke() throws IOException {
            inputPath = getTestCorpusPath();
            outputDir = inputPath + "-out";
            FileUtils.deleteDirectory(new File(outputDir));
            System.out.println("Input text: " + inputPath);
            System.out.println("Output directory: "+  outputDir);
            return this;
        }
    }
}