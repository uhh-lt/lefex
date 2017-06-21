package de.tudarmstadt.lt.jst.ExtractSynCon;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import java.io.File;

import static org.junit.Assert.*;


public class HadoopTest {
    /*
    * The test is used as an example of class usage without any checks.
    * */

    private void run(String inputPath) throws Exception {
        String outputDir = inputPath + "-out";
        FileUtils.deleteDirectory(new File(outputDir));
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " +  outputDir);

        Configuration conf = new Configuration();
        ToolRunner.run(conf, new HadoopMain(), new String[]{inputPath, outputDir});
        assertTrue("OK.", true);
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test/ukwac-sample-10.txt").getFile());
        String inputPath = file.getAbsolutePath();
        run(inputPath);
    }


}

