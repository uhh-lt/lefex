package de.tudarmstadt.lt.jst.Sandbox;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;
import de.tudarmstadt.lt.jst.Utils.Resources;
import de.tudarmstadt.lt.jst.Utils.Format;

import java.util.HashSet;

public class Worksheet {
    @Test
    public void ensureDirTest() throws Exception {
        Format.ensureDir("/Users/sasha/Desktop/mwe-c");
    }

    @Test
    public void containsTest() throws Exception {
        String holingType = "dependency+trigram";
        assertTrue("Substring works", holingType.contains("dependency"));
    }

    @Test
    public void equalsTest() throws Exception {
        String holingType = "dependency";
        assertTrue("Equals works", holingType.equals("dependency"));
    }

    @Test
    public void loadVocTest() {
        String mwePath = Resources.getJarResourcePath("data/voc-sample.csv");
        HashSet<String> h = Resources.loadVoc(mwePath);
        System.out.println(h);
        assertEquals(h.size(),10);
    }

    @Test
    public void getAbsolutePathTest() {
        System.out.println(Resources.getJarResourcePath("data/voc-sample.csv"));
    }
}