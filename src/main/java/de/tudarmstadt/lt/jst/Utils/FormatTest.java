package de.tudarmstadt.lt.jst.Utils;

import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

public class FormatTest {


    @Test
    public void readGzipAsListTest() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("test/simWithFeatures100.csv.gz").getFile());
        String inputPath = file.getAbsolutePath();

        List<String> l = Format.readGzipAsList(inputPath);
        String expectedValue = "zorros	FreeBSD	0.001	,_@_,";
        assertTrue("Should contain: ", l.contains(expectedValue));
    }

}
