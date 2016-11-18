package de.tudarmstadt.lt.jst.ExtractTermFeatureScores;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;
import java.io.File;
import java.io.IOException;
import java.util.*;
import de.tudarmstadt.lt.jst.Const;
import de.tudarmstadt.lt.jst.Utils.StanfordLemmatizer;
import de.tudarmstadt.lt.jst.Utils.Format;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.matetools.MateParser;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordParser;
import edu.stanford.nlp.util.Pair;
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
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import de.tudarmstadt.lt.jst.Utils.DictionaryAnnotator;

class HadoopMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final IntWritable ONE = new IntWritable(1);
    static final String POS_SEP = "#";
    static boolean VERBOSE = false;

    Logger log = Logger.getLogger("de.tudarmstadt.lt.jst");
	AnalysisEngine segmenter;
	AnalysisEngine posTagger;
	AnalysisEngine lemmatizer;
	AnalysisEngine depParser;
    AnalysisEngine dictTagger;
    AnalysisEngine nerEngine;

    JCas jCas;
    boolean semantifyDependencies;
	String holingType;
	boolean computeCoocs;
	int maxSentenceLength;
    boolean nounNounDependenciesOnly;
    boolean lemmatize;
    boolean mweByDicionary;
    int processEach;
    boolean useNgramSelfFeatures;
    boolean mweByNER;
    String depParserType;
    boolean useDependencyTypeStoplist;
    boolean outputPos;

	@Override
	public void setup(Context context) throws IOException {
        processEach = context.getConfiguration().getInt("holing.process_each", 1);
        log.info("Process each: " + processEach);

        String mwePath = "";
        if(context.getCacheFiles() != null && context.getCacheFiles().length > 0) mwePath = new File("./mwe_voc").getAbsolutePath();
        log.info("MWE vocabulary: " + mwePath);
        mweByDicionary = !mwePath.equals("");

        useNgramSelfFeatures = context.getConfiguration().getBoolean("holing.mwe.self_features", false);
        log.info("Use Ngram self features: " + useNgramSelfFeatures);

        mweByNER = context.getConfiguration().getBoolean("holing.mwe.ner", false);;
        log.info("Recognize named entities: " + mweByNER);

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

        depParserType = context.getConfiguration().getStrings("holing.dependencies.parser", "malt")[0];
        log.info("Dependency parser: " + depParserType);

        useDependencyTypeStoplist = context.getConfiguration().getBoolean("holing.dependency_stoplist", true);
        log.info("Use dependency type stoplist: " + useDependencyTypeStoplist);

        outputPos = context.getConfiguration().getBoolean("holing.output_pos", true);
        log.info("Output part-of-speech tags: " + outputPos);

        try {
			segmenter = AnalysisEngineFactory.createEngine(OpenNlpSegmenter.class);
			if (lemmatize) {
                posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
                lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            }
			if (holingType.equals("dependency")) synchronized(MaltParser.class) {
                if (depParserType.equals("malt")) depParser = AnalysisEngineFactory.createEngine(MaltParser.class);
                else if (depParserType.equals("mate")) depParser = AnalysisEngineFactory.createEngine(MateParser.class);
                else if (depParserType.equals("stanford")) depParser = AnalysisEngineFactory.createEngine(StanfordParser.class);
                else depParser = AnalysisEngineFactory.createEngine(MaltParser.class);
			}
            if(mweByDicionary && mwePath != ""){
                dictTagger = AnalysisEngineFactory.createEngine(DictionaryAnnotator.class,
                    DictionaryAnnotator.PARAM_ANNOTATION_TYPE, NamedEntity.class,
                    DictionaryAnnotator.PARAM_MODEL_LOCATION, mwePath,
                    DictionaryAnnotator.PARAM_EXTENDED_MATCH, "true");
            }
            if(mweByNER){
                nerEngine = AnalysisEngineFactory.createEngine(StanfordNamedEntityRecognizer.class);
            }

			jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
		} catch (ResourceInitializationException e) {
			log.error("Couldn't initialize analysis engine", e);
		} catch (CASException e) {
			log.error("Couldn't create new CAS", e);
		}
	}

    private List<NamedEntity> filterNgrams(List<NamedEntity> ngrams){
        List<NamedEntity> ngramsFiltered = new LinkedList<>();
        HashSet<Pair<Integer,Integer>> coveredRanges = new HashSet<>();
        for (NamedEntity ngram : ngrams) {
            Pair<Integer,Integer> ngramSpan = new Pair<Integer,Integer>(ngram.getBegin(),ngram.getEnd());
            if (!ngram.getCoveredText().contains(" ") || coveredRanges.contains(ngramSpan)) continue;

            coveredRanges.add(ngramSpan);
            ngramsFiltered.add(ngram);
        }
        return ngramsFiltered;
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
            if (mweByDicionary) dictTagger.process(jCas);
            if (mweByNER) nerEngine.process(jCas);
            if (holingType.equals("dependency")) depParser.process(jCas);

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());

                if (tokens.size() > maxSentenceLength) {
                    context.getCounter("de.tudarmstadt.lt.jst", "NUM_SKIPPED_SENTENCES").increment(1);
                    return;
                } else {
                    context.getCounter("de.tudarmstadt.lt.jst", "NUM_PROCESSED_SENTENCES").increment(1);
                }

                // W: word count -- single words
                Set<String> words = new HashSet<>();
                for (Token wordToken : tokens) {
                    String word;
                    if (lemmatize) word = wordToken.getLemma().getValue();
                    else word = wordToken.getCoveredText();
                    if (word == null) continue;
                    if (outputPos && lemmatize){
                        word = word + POS_SEP + wordToken.getPos().getPosValue();
                    }
                    context.write(new Text("W\t" + word), ONE);

                    if (computeCoocs) words.add(word);
                }

                // W: word count -- ngrams
                List<NamedEntity> ngrams = filterNgrams(JCasUtil.selectCovered(jCas, NamedEntity.class, sentence.getBegin(), sentence.getEnd()));
                for (NamedEntity ngram : ngrams) {
                    String ngramStr = "";
                    if (outputPos && lemmatize){
                        Collection<Token> ngramTokens = JCasUtil.selectCovered(jCas, Token.class, ngram.getBegin(), ngram.getEnd());
                        for (Token nt: ngramTokens){
                            ngramStr += nt.getCoveredText() + POS_SEP + nt.getPos().getPosValue() + " ";
                        }
                        ngramStr = ngramStr.trim();
                    } else {
                        ngramStr = ngram.getCoveredText();
                    }
                    context.write(new Text("W\t" + ngramStr), ONE);
                    if (computeCoocs) words.add(ngramStr);
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

                // WF and F: word-feature counts and feature counts
                if (holingType.equals("dependency")) {
                    dependencyHoling(context, tokens, ngrams, sentence.getBegin(), sentence.getEnd());
                } else if(holingType.equals("trigram")) {
                    trigramHoling(context, tokens, ngrams);
                } else {
                    dependencyHoling(context, tokens, ngrams, sentence.getBegin(), sentence.getEnd());
                }
            }
        } catch(Exception e){
            if (VERBOSE) log.error("Can't process line: " + value.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }

    private void trigramHoling(Context context, Collection<Token> tokens, List<NamedEntity> ngrams)
            throws AnalysisEngineProcessException, IOException, InterruptedException
    {
        try {
            String center = Const.BEGEND_CHAR;
            String left = Const.BEGEND_CHAR;
            String right = Const.BEGEND_CHAR;

            for (Token rightToken : tokens) {

                if (lemmatize && rightToken.getLemma() != null) right = rightToken.getLemma().getValue();
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

    private void dependencyHoling(Context context, Collection<Token> tokens, List<NamedEntity> ngrams, int beginSentence, int endSentence)
            throws AnalysisEngineProcessException, IOException, InterruptedException
    {
        Collection<Dependency> deps = JCasUtil.selectCovered(jCas, Dependency.class, beginSentence, endSentence);
        Collection<Dependency> depsCollapsed = Format.collapseDependencies(jCas, deps, tokens);
        for (Dependency dep : depsCollapsed) {
            // Get dependency
            Token governor = dep.getGovernor();
            Token dependent = dep.getDependent();
            String dtype = dep.getDependencyType();
            if (semantifyDependencies) dtype = Format.semantifyDependencyType(dtype);
            String governorPos = governor.getPos().getPosValue();
            String dependentPos = dependent.getPos().getPosValue();
            String governorLemma = governor.getLemma().getValue();
            String dependentLemma = dependent.getLemma().getValue();
            boolean skipByDependencyType = useDependencyTypeStoplist && Format.isStopDependencyType(dtype);
            if (governorLemma == null || dependentLemma == null || skipByDependencyType) continue;

            // Save the dependenc1y as a feature
            if (nounNounDependenciesOnly && (!governorPos.equals("NN") || !governorPos.equals("NNS"))) continue;
            String bim = dependent.getBegin() < governor.getBegin() ? dtype + "(" + dependentLemma + ",@)" : dtype + "(@," + dependentLemma + ")";
            context.write(new Text("F\t" + bim), ONE);
            String governorLemmaOut = outputPos ? governorLemma + POS_SEP + governorPos : governorLemma;
            context.write(new Text("WF\t" + governorLemmaOut + "\t" + bim), ONE); // part of speech is outputed only for words, not for features to reduce sparcity

            // Save inverse dependency as a feature
            if (nounNounDependenciesOnly && (!dependentPos.equals("NN") && !dependentPos.equals("NNS"))) continue;
            String ibim = dependent.getBegin() < governor.getBegin() ? dtype + "(@," + governorLemma + ")" : dtype + "(" + governorLemma + ",@)";

            context.write(new Text("F\t" + ibim), ONE);
            String dependentLemmaOut = outputPos ? dependentLemma + POS_SEP + dependentPos : dependentLemma;
            context.write(new Text("WF\t" + dependentLemmaOut + "\t" + ibim), ONE);

            // Generate features for multiword expressions
            String governorNgram = findNgram(ngrams, governor.getBegin(), governor.getEnd());
            String dependantNgram = findNgram(ngrams, dependent.getBegin(), dependent.getEnd());
            if (!governorNgram.equals("") && governorNgram.equals(dependantNgram) && !useNgramSelfFeatures) {
                // do not generate self-reference ngram features
            } else {
                if (!governorNgram.equals("")) context.write(new Text("WF\t" + governorNgram + "\t" + bim), ONE);
                if (!dependantNgram.equals("")) context.write(new Text("WF\t" + dependantNgram + "\t" + ibim), ONE);
            }

            context.progress();
        }
    }

    /**
     * If a token in the given range overlaps with an ngram => associate features of the token with the ngram.
     * */
    private String findNgram(List<NamedEntity> ngrams, int beginToken, int endToken){
        for (Annotation ngram : ngrams){
            if (ngram.getBegin() <= beginToken && ngram.getEnd() >= endToken) {
                if (outputPos && lemmatize) {
                    String ngramStr = "";
                    Collection<Token> ngramTokens = JCasUtil.selectCovered(jCas, Token.class, ngram.getBegin(), ngram.getEnd());
                    for (Token nt: ngramTokens){
                        ngramStr += nt.getCoveredText() + POS_SEP + nt.getPos().getPosValue() + " ";
                    }
                    return ngramStr.trim();
                } else {
                    return ngram.getCoveredText();
                }
            }
        }
        return "";
    }

}
