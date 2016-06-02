package de.tudarmstadt.lt.jst.ExtractTermFeatureScores;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.tudarmstadt.lt.jst.Const;
import de.tudarmstadt.lt.jst.Utils.StanfordLemmatizer;
import de.tudarmstadt.lt.jst.Utils.Format;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.NGram;
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
//import de.tudarmstadt.ukp.dkpro.core.dictionaryannotator.DictionaryAnnotator;
import de.tudarmstadt.lt.jst.Utils.DictionaryAnnotator;

class HadoopMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final IntWritable ONE = new IntWritable(1);

    Logger log = Logger.getLogger("de.tudarmstadt.lt.jst");
	AnalysisEngine segmenter;
	AnalysisEngine posTagger;
	AnalysisEngine lemmatizer;
	AnalysisEngine depParser;
    AnalysisEngine dictTagger;

    JCas jCas;
    boolean semantifyDependencies;
	String holingType;
	boolean computeCoocs;
	int maxSentenceLength;
    boolean nounNounDependenciesOnly;
    boolean lemmatize;
    boolean lookupMWE;
    int processEach;
    HashSet<String> mweVocabulary;

	@Override
	public void setup(Context context) {
        processEach = context.getConfiguration().getInt("holing.process_each", 1);
        log.info("Process each: " + processEach);

        String mwePath = context.getConfiguration().getStrings("holing.mwe.vocabulary", "")[0];
        log.info("MWE vocabulary: " + mwePath);
        lookupMWE = !mwePath.equals("");

        computeCoocs = context.getConfiguration().getBoolean("holing.coocs", false);
        log.info("Computing coocs: " + computeCoocs);

        maxSentenceLength = context.getConfiguration().getInt("holing.sentences.maxlength", 100);
        log.info("Sentences max length: " +  maxSentenceLength);

        holingType = context.getConfiguration().getStrings("holing.type", "dependency")[0];
        log.info("Holing type: " + holingType);

        semantifyDependencies = context.getConfiguration().getBoolean("holing.dependencies.semantify", true);
        log.info("Semantifying dependencies: " + semantifyDependencies);

        lemmatize = context.getConfiguration().getBoolean("holing.lemmatize", true);
        log.info("Lemmatize: " + lemmatize);

        nounNounDependenciesOnly = context.getConfiguration().getBoolean("holing.dependencies.noun_noun_dependencies_only", false);
        log.info("Noun-noun dependencies only: " + nounNounDependenciesOnly);

        try {
			segmenter = AnalysisEngineFactory.createEngine(OpenNlpSegmenter.class);
			if (lemmatize) {
                posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
                lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            }
			if (holingType.equals("dependency")) synchronized(MaltParser.class) {
				depParser = AnalysisEngineFactory.createEngine(MaltParser.class);
			}
            if(lookupMWE){
                dictTagger = AnalysisEngineFactory.createEngine(DictionaryAnnotator.class,
                    DictionaryAnnotator.PARAM_ANNOTATION_TYPE, NGram.class,
                    DictionaryAnnotator.PARAM_MODEL_LOCATION, mwePath,
                    DictionaryAnnotator.PARAM_EXTENDED_MATCH, "true");
            }

			jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
		} catch (ResourceInitializationException e) {
			log.error("Couldn't initialize analysis engine", e);
		} catch (CASException e) {
			log.error("Couldn't create new CAS", e);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter("de.tudarmstadt.lt.jst", "NUM_MAPS").increment(1);
        if (context.getCounter("de.tudarmstadt.lt.jst", "NUM_MAPS").getValue() % processEach != 0) return;

        try {
            String text = value.toString();
            jCas.reset();
            jCas.setDocumentText(text);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);
            if(lemmatize) {
                posTagger.process(jCas);
                lemmatizer.process(jCas);
            }
            if (lookupMWE) dictTagger.process(jCas);
            if (holingType.equals("dependency")) depParser.process(jCas);

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());

                if (tokens.size() > maxSentenceLength) {
                    context.getCounter("de.tudarmstadt.lt.jst", "NUM_SKIPPED_SENTENCES").increment(1);
                    return;
                } else {
                    context.getCounter("de.tudarmstadt.lt.jst", "NUM_PROCESSED_SENTENCES").increment(1);
                }

                // W: word count
                Set<String> words = new HashSet<>();
                for (Token wordToken : tokens) {
                    String word;
                    if (lemmatize) word = wordToken.getLemma().getValue();
                    else word = wordToken.getCoveredText();

                    if (word == null) continue;
                    if (computeCoocs) words.add(word);
                    context.write(new Text("W\t" + word), ONE);
                }

                List<NGram> ngrams = JCasUtil.selectCovered(jCas, NGram.class, sentence.getBegin(), sentence.getEnd());
                for (NGram mwe : ngrams) {
                    if (computeCoocs) words.add(mwe.getCoveredText());
                    context.write(new Text("W\t" + mwe.getCoveredText()), ONE);
                    //System.out.println(">>>>>>>>>> " + mwe.getCoveredText());
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
                    dependencyHoling(context, tokens, ngrams, sentence.getBegin(), sentence.getEnd());
                } else if(holingType.equals("trigram")) {
                    trigramHoling(context, tokens);
                } else {
                    dependencyHoling(context, tokens, ngrams, sentence.getBegin(), sentence.getEnd());
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
            context.getCounter("de.tudarmstadt.lt.jst", "HOLING_EXCEPTIONS").increment(1);
        }
    }

    private String findNgram(List<NGram> ngrams, int beginSpan, int endSpan){
        for (NGram ngram : ngrams){
            if (ngram.getBegin() >= beginSpan && ngram.getEnd() >= endSpan) return ngram.getCoveredText();
        }
        return "";
    }

    private void dependencyHoling(Context context, Collection<Token> tokens, List<NGram> ngrams, int beginSentence, int endSentence)
            throws AnalysisEngineProcessException, IOException, InterruptedException
    {
        Collection<Dependency> deps = JCasUtil.selectCovered(jCas, Dependency.class, beginSentence, endSentence);
        Collection<Dependency> depsCollapsed = Format.collapseDependencies(jCas, deps, tokens);
        for (Dependency dep : depsCollapsed) {
            // Get dependency
            Token governor = dep.getGovernor();
            Token dependent = dep.getDependent();
            String rel = dep.getDependencyType();
            if (semantifyDependencies) rel = Format.semantifyDependencyRelation(rel);
            String governorPos = governor.getPos().getPosValue();
            String dependentPos = dependent.getPos().getPosValue();
            String governorLemma = governor.getLemma().getValue();
            String dependentLemma = dependent.getLemma().getValue();
            if (governorLemma == null || dependentLemma == null) continue;

            // Save the dependency as a feature
            if (nounNounDependenciesOnly && (!governorPos.equals("NN") || !governorPos.equals("NNS"))) continue;
            String bim = dependent.getBegin() < governor.getBegin() ? rel + "(" + dependentLemma + ",@)" : rel + "(@," + dependentLemma + ")";
            context.write(new Text("F\t" + bim), ONE);
            context.write(new Text("WF\t" + governorLemma + "\t" + bim), ONE);

            // Save inverse dependency as a feature
            if (nounNounDependenciesOnly && (!dependentPos.equals("NN") && !dependentPos.equals("NNS"))) continue;
            String ibim = dependent.getBegin() < governor.getBegin() ? rel + "(@," + governorLemma + ")" : rel + "(" + governorLemma + ",@)";

            context.write(new Text("F\t" + ibim), ONE);
            context.write(new Text("WF\t" + dependentLemma + "\t" + ibim), ONE);

            // Generate features for multiword expressions
            String governorNgram = findNgram(ngrams, governor.getBegin(), governor.getEnd());
            String dependantNgram = findNgram(ngrams, governor.getBegin(), governor.getEnd());
            if (!governorNgram.equals("")) {
                context.write(new Text("WF\t" + governorNgram + "\t" + bim), ONE);
            if (!dependantNgram.equals("")) {
                context.write(new Text("WF\t" + dependantNgram + "\t" + ibim), ONE);
            }

            context.progress();
        }
    }
}