package de.tudarmstadt.lt.jst;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;

public class TestPaths {
    public TestPaths() { }

    public TestPaths(String corpusType) {
        try {
            if (corpusType.equals("ner")) inputPath = getNerTestCorpusPath();
            else if (corpusType.equals("large")) inputPath = "/Users/panchenko/work/tmp/large-corpus.txt"; // only for local tests
            else inputPath = getTestCorpusPath();
            outputDir = inputPath + "-out";
            FileUtils.deleteDirectory(new File(outputDir));
            System.out.println("Input text: " + inputPath);
            System.out.println("Output directory: " + outputDir);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private String inputPath;
    private String outputDir;

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public String getTestCorpusPath() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ukwac-sample-10.txt").getFile());
        return file.getAbsolutePath();
    }

    public String getNerTestCorpusPath() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("data/ner-error-text.txt").getFile());
        return file.getAbsolutePath();
    }
}