package de.tudarmstadt.lt.jst;


import org.junit.Test;
import static org.junit.Assert.*;


public class Worksheet {
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
}