package de.tudarmstadt.lt.jst.CoNLL;

import de.tudarmstadt.lt.jst.Utils.Format;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordSegmenter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

public class HadoopMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
    AnalysisEngine segmenter;
    AnalysisEngine posTagger;
    AnalysisEngine lemmatizer;
    AnalysisEngine parser;
    AnalysisEngine nerEngine;
    JCas jCas;
    boolean collapsing;
    String parserName;

    @Override
    public void setup(Context context) throws IOException {
        parserName = context.getConfiguration().getStrings("parserName", "malt")[0];
        log.info("Parser: " + parserName);

        collapsing = context.getConfiguration().getBoolean("collapsing", true);
        log.info("Collapse dependencies: " + collapsing);

        try {
            segmenter = AnalysisEngineFactory.createEngine(StanfordSegmenter.class);
            posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
            lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            if (parserName.toLowerCase().contains("malt")) {
                synchronized (MaltParser.class) {
                    parser = AnalysisEngineFactory.createEngine(MaltParser.class);
                }
            }
            // Not implemented
            //else if (parserName.toLowerCase().contains("standord")) {
            //    // Initialize the stanford parser
            //}
            else {
                synchronized (MaltParser.class) {
                    parser = AnalysisEngineFactory.createEngine(MaltParser.class);
                }
            }

            nerEngine = AnalysisEngineFactory.createEngine(StanfordNamedEntityRecognizer.class,
                    StanfordNamedEntityRecognizer.PARAM_LANGUAGE, "en",
                    StanfordNamedEntityRecognizer.PARAM_VARIANT, "all.3class.distsim.crf");

            jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
        } catch (ResourceInitializationException e) {
            log.error("Couldn't initialize analysis engine", e);
        } catch (CASException e) {
            log.error("Couldn't create new CAS", e);
        }
    }

    public static String getShortName(String dkproType){
        if (dkproType.length() > 0){
            String [] fields = dkproType.split("\\.");
            if (fields.length > 0){
                return fields[fields.length-1];
            } else {
                return dkproType;
            }
        } else {
            return dkproType;
        }
    }

    private String getBIO(List<NamedEntity> ngrams, int beginToken, int endToken){
        for (NamedEntity ngram : ngrams){
            if (ngram.getBegin() == beginToken) {
                return "B-" + getShortName(ngram.getType().getName());
            } else if (ngram.getBegin() < beginToken && ngram.getEnd() >= endToken) {
                return "I-" + getShortName(ngram.getType().getName());
            } else {
                return "O";
            }
        }
        return "O";
    }

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        try {
            jCas.reset();
            jCas.setDocumentText(line.toString());
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);
            posTagger.process(jCas);
            lemmatizer.process(jCas);
            nerEngine.process(jCas);
            parser.process(jCas);

            // For each dependency output a field with ten columns ending with the bio named entity: http://universaldependencies.org/docs/format.html
            // IN_ID TOKEN LEMMA POS_COARSE POS_FULL MORPH ID_OUT TYPE _ NE_BIO
            // An example is below. NB: the last (10th) field can be anything according to the specification (NE in our case)
            // 5 books book NOUN NNS Number=Plur 2 dobj 4:dobj SpaceAfter=No

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());
                HashMap<String, Integer> tokenToID = collectionToMap(tokens);
                List<NamedEntity> ngrams = JCasUtil.selectCovered(jCas, NamedEntity.class, sentence);

                context.getCounter("de.tudarmstadt.lt.wsi", "NUM_PROCESSED_SENTENCES").increment(1);
                context.write(new Text("-1\t" + sentence.getCoveredText()), NullWritable.get());
                Collection<Dependency> deps = JCasUtil.selectCovered(jCas, Dependency.class, sentence.getBegin(), sentence.getEnd());
                if (collapsing) deps = Format.collapseDependencies(jCas, deps, tokens);

                TreeMap<Integer, String> conllLines = new TreeMap<>();
                for (Dependency dep : deps) {
                    Integer id = tokenToID.getOrDefault(dep.getDependent().getCoveredText(), -2);
                    String BIO = getBIO(ngrams, dep.getBegin(), dep.getEnd());
                    String conllLine = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s",
                            dep.getDependent().getCoveredText(),
                            dep.getDependent().getLemma() != null ? dep.getDependent().getLemma().getValue() : "",
                            dep.getDependent().getPos() != null ? dep.getDependent().getPos().getPosValue() : "",
                            dep.getDependent().getPos() != null ? dep.getDependent().getPos().getPosValue() : "",
                            dep.getDependent().getMorph() != null ? dep.getDependent().getMorph().getValue() : "",
                            tokenToID.getOrDefault(dep.getGovernor().getCoveredText(), -2),
                            dep.getDependencyType(),
                            "_",
                            BIO
                    );
                    conllLines.put(id, conllLine);
                }

                for (Integer id : conllLines.keySet()) {
                    String res = String.format("%d\t%s", id, conllLines.get(id));
                    context.write(new Text(res), NullWritable.get());
                }
            }
        } catch(Exception e){
            log.error("Can't process line: " + line.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }

    private HashMap<String, Integer> collectionToMap(Collection<Token> tokens){
        HashMap<String,Integer> token2id = new HashMap<>();
        Integer id = 0;
        for (Token t : tokens){
            token2id.put(t.getCoveredText(), id);
            id++;
        }
        return token2id;
    }
}

