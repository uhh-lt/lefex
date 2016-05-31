package de.tudarmstadt.lt.wsi;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

class JoBimExtractAndCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final IntWritable ONE = new IntWritable(1);

    Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
	AnalysisEngine segmenter;
	AnalysisEngine posTagger;
	AnalysisEngine lemmatizer;
	AnalysisEngine depParser;
	JCas jCas;
	boolean semantifyDependencies;
	String holingType;
	boolean computeCoocs;
	int maxSentenceLength;
	boolean nounsOnly;
    boolean nounNounDependenciesOnly;
    boolean lemmatize;
    int processEach;

	@Override
	public void setup(Context context) {
        processEach = context.getConfiguration().getInt("holing.processeach", 1);
        log.info("Process each: " + processEach);

		computeCoocs = context.getConfiguration().getBoolean("holing.coocs", false);
        log.info("Computing coocs: " + computeCoocs);

        maxSentenceLength = context.getConfiguration().getInt("holing.sentences.maxlength", 100);
        log.info("Sentences max length: " +  maxSentenceLength);

        holingType = context.getConfiguration().getStrings("holing.type", "dependency")[0];
        log.info("Holing type: " + holingType);

        semantifyDependencies = context.getConfiguration().getBoolean("holing.dependencies.semantify", true);
        log.info("Semantifying dependencies: " + semantifyDependencies);

        nounsOnly = context.getConfiguration().getBoolean("holing.nouns_only", false);
        log.info("Nouns only: " + nounsOnly);

        lemmatize = context.getConfiguration().getBoolean("holing.lemmatize", true);
        log.info("Lemmatize: " + lemmatize);

        nounNounDependenciesOnly = context.getConfiguration().getBoolean("holing.dependencies.noun_noun_dependencies_only", false);
        log.info("Noun-noun dependencies only: " + nounNounDependenciesOnly);

        try {
			segmenter = AnalysisEngineFactory.createEngine(OpenNlpSegmenter.class);
			//if (lemmatize) {
                posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
                lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            //}
			if (holingType.equals("dependency")) synchronized(MaltParser.class) {
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
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

        context.getCounter("de.tudarmstadt.lt.wsi", "NUM_MAPS").increment(1);
        if (context.getCounter("de.tudarmstadt.lt.wsi", "NUM_MAPS").getValue() % processEach != 0) return;

        try {
            String text = value.toString();
            jCas.reset();
            jCas.setDocumentText(text);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());

                if (tokens.size() > maxSentenceLength) {
                    context.getCounter("de.tudarmstadt.lt.wsi", "NUM_SKIPPED_SENTENCES").increment(1);
                    return;
                } else {
                    context.getCounter("de.tudarmstadt.lt.wsi", "NUM_PROCESSED_SENTENCES").increment(1);
                }
                if(lemmatize) {
                    posTagger.process(jCas);
                    lemmatizer.process(jCas);
                }

                Set<String> words = new HashSet<>();
                for (Token wordToken : tokens) {
                    String word;
                    if (lemmatize) word = wordToken.getLemma().getValue();
                    else word = wordToken.getCoveredText();
                    if (word == null) continue;

                    words.add(word);

                    context.write(new Text("W\t" + word), ONE);
                    if(lemmatize) {
                        String pos = wordToken.getPos().getPosValue();
                        if (nounsOnly && (!pos.equals("NN") || !pos.equals("NNS"))) continue;
                        context.write(new Text("WNouns\t" + word), ONE);
                    }
                }

                if (computeCoocs) {
                    for (String word_i : words) {
                        context.write(new Text("CoocF\t" + word_i), ONE);
                        for (String word_j : words) {
                            context.write(new Text("CoocWF\t" + word_i + "\t" + word_j), ONE);
                        }
                        context.progress();
                    }
                }

                if (holingType.equals("dependency")) {
                    dependencyHoling(context, tokens);
                } else if(holingType.equals("trigram")) {
                    trigramHoling(context, tokens);
                } else {
                    dependencyHoling(context, tokens);
                }
            }
        } catch(Exception e){
            log.error("Can't process line: " + value.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }

    private void trigramHoling(Context context, Collection<Token> tokens) throws AnalysisEngineProcessException, IOException, InterruptedException {
        try {
            String center = Const.BEGEND_CHAR;
            String left = Const.BEGEND_CHAR;
            String right = Const.BEGEND_CHAR;

            for (Token rightToken : tokens) {
                if (lemmatize) right = rightToken.getLemma().getValue();
                else right = rightToken.getCoveredText();
                if (right == null) continue;

                if (!right.equals(Const.BEGEND_CHAR) && !center.equals(Const.BEGEND_CHAR)) {
                    String bim = left + "_@_" + right;
                    context.write(new Text("F\t" + bim), ONE);
                    context.write(new Text("WF\t" + center + "\t" + bim), ONE);
                    context.progress();
                }

                left = center;
                center = right;
            }

            if (!right.equals(Const.BEGEND_CHAR)) {
                String bim = left + "_@_" + Const.BEGEND_CHAR;
                context.write(new Text("F\t" + bim), ONE);
                context.write(new Text("WF\t" + right + "\t" + bim), ONE);
                context.progress();
            }
        } catch (Exception exc) {
            context.getCounter("de.tudarmstadt.lt.wsi", "HOLING_EXCEPTIONS").increment(1);
        }
    }

    private void dependencyHoling(Context context, Collection<Token> tokens) throws AnalysisEngineProcessException, IOException, InterruptedException {
        depParser.process(jCas);
        Collection<Dependency> deps = JCasUtil.select(jCas, Dependency.class);
        Collection<Dependency> depsCollapsed = Util.collapseDependencies(jCas, deps, tokens);
        for (Dependency dep : depsCollapsed) {
            // Get dependency
            Token source = dep.getGovernor();
            Token target = dep.getDependent();
            String rel = dep.getDependencyType();
            if (semantifyDependencies) rel = Util.semantifyDependencyRelation(rel);
            String sourcePos = source.getPos().getPosValue();
            String targetPos = target.getPos().getPosValue();
            String sourceLemma = source.getLemma().getValue();
            String targetLemma = target.getLemma().getValue();
            if (sourceLemma == null || targetLemma == null) continue;

            // Save the dependency as a feature
            if (nounNounDependenciesOnly && (!sourcePos.equals("NN") || !sourcePos.equals("NNS"))) continue;
            String bim = target.getBegin() < source.getBegin() ? rel + "(" + targetLemma + ",@)" : rel + "(@," + targetLemma + ")";
            context.write(new Text("F\t" + bim), ONE);
            context.write(new Text("WF\t" + sourceLemma + "\t" + bim), ONE);

            // Save inverse dependency as a feature
            if (nounNounDependenciesOnly && (!targetPos.equals("NN") && !targetPos.equals("NNS"))) continue;
            String ibim = target.getBegin() < source.getBegin() ? rel + "(@," + sourceLemma + ")" : rel + "(" + sourceLemma + ",@)";

            context.write(new Text("F\t" + ibim), ONE);
            context.write(new Text("WF\t" + targetLemma + "\t" + ibim), ONE);
            context.progress();
        }
    }
}
