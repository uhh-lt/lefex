package de.tudarmstadt.lt.jst.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Resources{
    public static String getJarResourcePath(String relativePath) {
        Resources r = new Resources();
        ClassLoader classLoader = r.getClass().getClassLoader();
        File file = new File(classLoader.getResource(relativePath).getFile());
        return file.getAbsolutePath();
    }

    public static HashSet<String> loadVoc(String mwePath) {
        HashSet<String> voc = new HashSet<>();
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(mwePath))));

            String line;
            line = br.readLine();


            while (line != null) {
                voc.add(line.trim());
                line = br.readLine();
            }
        } finally {
            return voc;
        }
    }
}