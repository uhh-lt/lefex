package de.uhh.lt.lefex.Utils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.uima.jcas.JCas;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import org.apache.uima.jcas.tcas.Annotation;


public class Format {

    private static String NOT_SEP = ";";
    private static String NODES = "nodes.csv";
    private static String EDGES = "edges.csv";


    public static void ensureDir(String directoryPath){
        File dir = new File(directoryPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public static void saveDependenciesGephiCSV(Collection<Dependency> deps, String outputDir) {
        ensureDir(outputDir);

        // Edges file
        HashSet<String> nodes = new HashSet<>();
        try (Writer edgesFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDir + "/" + EDGES), "utf-8"))) {
            edgesFile.write("Source,Target,Type,Id,Label,Weight\n");
            int id = 0;
            for (Dependency dep : deps) {
                String src = dep.getGovernor().getCoveredText().replace(",", ";") + "-" + dep.getGovernor().getBegin();
                String dst = dep.getDependent().getCoveredText().replace(",", ";") + "-" + dep.getDependent().getBegin();
                String label = dep.getDependencyType();
                edgesFile.write(String.format("%s,%s,Directed,%d,%s,1.0\n", src, dst, ++id, label));
                nodes.add(src);
                nodes.add(dst);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // Nodes file
        try (Writer nodesFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDir + "/" + NODES), "utf-8"))) {
            nodesFile.write("Id,Label\n");
            int i = 1;
            for (String node : nodes) {
                nodesFile.write(String.format("%s,%s\n", node, node));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String trimScore(String score) {
        String[] parts = score.split("[:#]");
        return parts[0];
    }

    public static String flatten(List<String> list, int maxLength, boolean keepScores, String sep) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String str : list) {
            if (i > maxLength) break;

            str = str.replace(",", NOT_SEP);
            str = str.replace(sep, NOT_SEP);
            if (keepScores) sb.append(str);
            else sb.append(trimScore(str));
            sb.append(sep);
            i++;
        }
        return sb.toString().trim();
    }

    public static Collection<Dependency> collapseDependencies(JCas jCas, Collection<Dependency> deps, Collection<Token> tokens) {
        List<Dependency> collapsedDeps = new ArrayList<>(deps);
        for (Token token : tokens) {
            if (token.getPos() != null && token.getPos().getPosValue().equals("IN")) {
                List<Dependency> toRemove = new ArrayList<>();
                String depType = "prep_" + token.getCoveredText().toLowerCase();
                Token source = null;
                Token target = null;
                int begin = -1;
                int end = -1;
                for (Dependency dep : collapsedDeps) {
                    if (dep.getGovernor() == token && dep.getDependencyType().toLowerCase().equals("mwe")) {
                        depType = "prep_" + dep.getDependent().getCoveredText() + "_" + token.getCoveredText().toLowerCase();
                        toRemove.add(dep);
                    } else if (dep.getGovernor() == token && dep.getDependencyType().toLowerCase().equals("pobj")) {
                        end = dep.getEnd();
                        target = dep.getDependent();
                        toRemove.add(dep);
                    } else if (dep.getDependent() == token && dep.getDependencyType().toLowerCase().equals("prep")) {
                        begin = dep.getBegin();
                        source = dep.getGovernor();
                        toRemove.add(dep);
                    }
                }
                if (source != null && target != null) {
                    Dependency collapsedDep = new Dependency(jCas, begin, end);
                    collapsedDep.setGovernor(source);
                    collapsedDep.setDependent(target);
                    collapsedDep.setDependencyType(depType);
                    collapsedDeps.add(collapsedDep);
                    collapsedDeps.removeAll(toRemove);
                }
            }
        }
        return collapsedDeps;
    }

    public static String semantifyDependencyType(String rel) {
        switch (rel) {
            case "nsubj":
                return "subj";
            case "nsubjpass":
            case "partmod":
            case "infmod":
            case "vmod":
            case "dobj":
                return "obj";
        }
        return rel;
    }

    private static HashSet<String> _stopDependencies = new HashSet<>(Arrays.asList("root"));

    public static boolean isStopDependencyType(String dtype){
        return _stopDependencies.contains(dtype.toLowerCase());
    }

    public static String join(List<String> list, String sep) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String str : list) {
            i++;
            if (!str.equals("") && !str.contains(sep)) {
                sb.append(str);
                if (i < list.size()) sb.append(sep);
            }
        }
        return sb.toString().trim();
    }

    public static void printCasAnnotations(JCas jCas) {
        System.out.println("All annotations of the JCas");
        for (Annotation a : jCas.getAnnotationIndex()) {
            System.out.println(a.getType().toString());
        }
    }

    public static List<String> readAsList(String filePath) throws IOException {
        InputStream fileStream = new FileInputStream(filePath);
        Reader decoder = new InputStreamReader(fileStream, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(decoder);

        String line;
        List<String> res = new LinkedList<>();
        while ((line = br.readLine()) != null) {
            res.add(line);
        }
        return res;
    }

    public static List<String> readGzipAsList(String gzipPath) throws IOException {
        InputStream fileStream = new FileInputStream(gzipPath);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(decoder);

        String line;
        List<String> res = new LinkedList<>();
        while ((line = br.readLine()) != null) {
            res.add(line);
        }
        return res;
    }
}
