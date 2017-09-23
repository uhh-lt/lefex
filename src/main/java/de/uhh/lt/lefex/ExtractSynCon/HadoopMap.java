package de.uhh.lt.lefex.ExtractSynCon;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;
import java.io.IOException;
import java.util.*;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;
import de.uhh.lt.lefex.Utils.Format;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.uhh.lt.lefex.Utils.DefaultDict;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
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


public class HadoopMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final IntWritable ONE = new IntWritable(1);

    Logger log = Logger.getLogger("de.uhh.lt.lefex");
    AnalysisEngine segmenter;
    AnalysisEngine posTagger;
    AnalysisEngine lemmatizer;
    AnalysisEngine depParser;
    JCas jCas;
    boolean collapseDependencies;
    boolean lemmatize;

    @Override
    public void setup(Context context) {
        collapseDependencies = context.getConfiguration().getBoolean("holing.dependencies.semantify", true);
        log.info("Semantifying dependencies: " + collapseDependencies);
        lemmatize = context.getConfiguration().getBoolean("holing.lemmatize", true);
        log.info("Lemmatize: " + lemmatize);

        try {
            segmenter = AnalysisEngineFactory.createEngine(OpenNlpSegmenter.class);
            posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
            lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            depParser = AnalysisEngineFactory.createEngine(MaltParser.class);
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
            String text = line.toString();
            jCas.reset();
            jCas.setDocumentText(text);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);
            context.write(line, ONE);

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());
                context.getCounter("de.uhh.lt.lefex", "NUM_PROCESSED_SENTENCES").increment(1);
                posTagger.process(jCas);
                lemmatizer.process(jCas);
                depParser.process(jCas);

                DefaultDict<String, List<String>> depsMap = new DefaultDict<>(LinkedList.class);
                Collection<Dependency> deps = JCasUtil.selectCovered(jCas, Dependency.class, sentence.getBegin(), sentence.getEnd());
                if (collapseDependencies) deps = Format.collapseDependencies(jCas, deps, tokens);
                for (Dependency dep : deps) {
                    String source = dep.getGovernor().getLemma().getValue() + dep.getGovernor().getBegin();
                    String target = dep.getDependent().getLemma().getValue() + dep.getDependent().getBegin();
                    String type = dep.getDependencyType();
                    if (collapseDependencies) type = Format.semantifyDependencyType(type);
                    String dirDependency = "";
                    String invDependency = "";
                    dirDependency = type + ":@:" + dep.getDependent().getLemma().getValue();  // new simpler to parse format "type:src:dst
                    invDependency = type + ":" + dep.getGovernor().getLemma().getValue() + ":@";
                    depsMap.get(source).add(dirDependency);
                    depsMap.get(target).add(invDependency);
                }

                for (Token token : tokens) {
                    String surface = text.substring(token.getBegin(), token.getEnd());
                    String lemma = token.getLemma().getValue();
                    String pos = token.getPos().getPosValue();
                    String depsStr = Format.flatten(depsMap.get(lemma + token.getBegin()), 100, true, ",");
                    context.write(new Text(String.format("%s\t%s\t%s\t%s",surface, lemma, pos, depsStr)), ONE);
                }
            }
        } catch (Exception e) {
            log.error("Can't process line: " + line.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }
}


