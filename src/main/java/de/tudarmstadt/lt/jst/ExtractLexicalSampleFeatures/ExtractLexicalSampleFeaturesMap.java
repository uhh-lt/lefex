package de.tudarmstadt.lt.jst.ExtractLexicalSampleFeatures;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;
import java.io.IOException;
import java.util.*;

import de.tudarmstadt.lt.jst.Const;
import de.tudarmstadt.lt.jst.Utils.StanfordLemmatizer;
import de.tudarmstadt.lt.jst.Utils.Format;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;

public class ExtractLexicalSampleFeaturesMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final IntWritable ONE = new IntWritable(1);

    Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
    AnalysisEngine segmenter;
    AnalysisEngine posTagger;
    AnalysisEngine lemmatizer;
    AnalysisEngine depParser;
    JCas jCas;
    boolean semantifyDependencies;
    String holingType;
    boolean lemmatize;

    @Override
    public void setup(Context context) {
        holingType = context.getConfiguration().getStrings("holing.type", "dependency")[0];
        log.info("Holing type: " + holingType);

        semantifyDependencies = context.getConfiguration().getBoolean("holing.dependencies.semantify", true);
        log.info("Semantifying dependencies: " + semantifyDependencies);

        lemmatize = context.getConfiguration().getBoolean("holing.lemmatize", true);
        log.info("Lemmatize: " + lemmatize);

        try {
            segmenter = AnalysisEngineFactory.createEngine(OpenNlpSegmenter.class);
            posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
            lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            if (holingType.contains("dependency")) synchronized(MaltParser.class) {
                depParser = AnalysisEngineFactory.createEngine(MaltParser.class);
            }
            jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
        } catch (ResourceInitializationException e) {
            log.error("Couldn't initialize analysis engine", e);
        } catch (CASException e) {
            log.error("Couldn't create new CAS", e);
        }
    }

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        try {
            LexicalSampleDataset lexSample = new LexicalSampleDataset(line.toString());
            jCas.reset();
            jCas.setDocumentText(lexSample.context);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);

            List<String> wordFeatures = new LinkedList<>();
            List<String> holingAllFeatures = new LinkedList <>();
            List<String> holingTargetFeatures = new LinkedList <>();

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                // Increment word features
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());
                context.getCounter("de.tudarmstadt.lt.wsi", "NUM_PROCESSED_SENTENCES").increment(1);
                if(lemmatize) {
                    posTagger.process(jCas);
                    lemmatizer.process(jCas);
                }

                for (Token wordToken : tokens) {
                    String word;
                    if (lemmatize) word = wordToken.getLemma().getValue();
                    else word = wordToken.getCoveredText();
                    if (word == null || word.equals(",") || word.equals(".") || word.equals(";") || word.equals("\"") || word.equals("'")) continue;
                    wordFeatures.add(word);
                }

                // Increment holing features
                if (holingType.contains("dependency")) {
                    HolingResult res = dependencyHoling(tokens, lexSample.target);
                    holingAllFeatures.addAll(res.allFeatures);
                    holingTargetFeatures.addAll(res.targetFeatures);
                }

                if(holingType.contains("trigram")) {
                    HolingResult res = trigramHoling(tokens, lexSample.target);
                    holingAllFeatures.addAll(res.allFeatures);
                    holingTargetFeatures.addAll(res.targetFeatures);
                }
            }

            lexSample.setFeatures(wordFeatures, holingAllFeatures, holingTargetFeatures);
            context.write(new Text(lexSample.asString()), ONE);

        } catch(Exception e){
            log.error("Can't process line: " + line.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }

    private HolingResult trigramHoling(Collection<Token> tokens, String lexSampleTarget) throws AnalysisEngineProcessException, IOException, InterruptedException {
        String center = Const.BEGEND_CHAR;
        String left = Const.BEGEND_CHAR;
        String right = Const.BEGEND_CHAR;
        List<String> allFeatures = new LinkedList<>();
        List<String> targetFeatures = new LinkedList<>();

        for (Token rightToken : tokens) {
            if (lemmatize) right = rightToken.getLemma().getValue();
            else right = rightToken.getCoveredText();
            if (right == null) continue;

            if (!right.equals(Const.BEGEND_CHAR) && !center.equals(Const.BEGEND_CHAR)) {
                String bim = left + "_@_" + right;
                allFeatures.add(bim);
                if (center.toLowerCase().equals(lexSampleTarget.toLowerCase())){
                    targetFeatures.add(bim);
                }
            }
            left = center;
            center = right;
        }

        if (!right.equals(Const.BEGEND_CHAR)) {
            String bim = left + "_@_" + Const.BEGEND_CHAR;
            allFeatures.add(bim);
        }

        return new HolingResult(allFeatures, targetFeatures);
    }

    private HolingResult dependencyHoling(Collection<Token> tokens, String lexSampleTarget) throws AnalysisEngineProcessException, IOException, InterruptedException {
        List<String> allFeatures = new LinkedList<>();
        List<String> targetFeatures = new LinkedList<>();
        depParser.process(jCas);
        Collection<Dependency> deps = JCasUtil.select(jCas, Dependency.class);
        Collection<Dependency> depsCollapsed = Format.collapseDependencies(jCas, deps, tokens);
        for (Dependency dep : depsCollapsed) {

            // Get dependency
            Token source = dep.getGovernor();
            Token target = dep.getDependent();
            String rel = dep.getDependencyType();
            if (semantifyDependencies) rel = Format.semantifyDependencyRelation(rel);
            String sourceLemma = source.getLemma().getValue();
            String targetLemma = target.getLemma().getValue();
            if (sourceLemma == null || targetLemma == null) continue;

            // Save dependency features, position of hole is always as in the text
            String bim = target.getBegin() < source.getBegin() ? rel + "(" + targetLemma + ",@)" : rel + "(@," + targetLemma + ")";  //
            allFeatures.add(bim);
            if (sourceLemma.toLowerCase().equals(lexSampleTarget.toLowerCase())){
                targetFeatures.add(bim);
            }

            String ibim = target.getBegin() < source.getBegin() ? rel + "(@," + sourceLemma + ")" : rel + "(" + sourceLemma + ",@)";
            allFeatures.add(ibim);
            if (targetLemma.toLowerCase().equals(lexSampleTarget.toLowerCase())){
                targetFeatures.add(ibim);
            }
        }

        return new HolingResult(allFeatures, targetFeatures);
    }
}

