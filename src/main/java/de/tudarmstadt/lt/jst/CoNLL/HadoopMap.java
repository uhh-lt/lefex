package de.tudarmstadt.lt.jst.CoNLL;

import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordSegmenter;
import org.apache.commons.io.IOUtils;
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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordParser;
import org.jsoup.Jsoup;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;
import org.jobimtext.collapsing.annotator.CollapsedDependenciesAnnotator;
import org.jobimtext.collapsing.type.NewCollapsedDependency;


public class HadoopMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
    AnalysisEngine segmenter;
    AnalysisEngine posTagger;
    AnalysisEngine lemmatizer;
    AnalysisEngine parser;
    AnalysisEngine collapser;
    AnalysisEngine nerEngine;
    JCas jCas;
    boolean collapsing;
    String parserName;
    boolean verbose = false;
    int maxSentenceSizeTokens = 50;
    String inputType;
    String SENTENCE = "sentence";
    String DOCUMENT = "document";
    Pattern urlRegex = Pattern.compile("(http://|www\\.|[a-z0-9]\\.com)");
    Pattern htmlRegex = Pattern.compile("<[a-z ='\"/:0-9]+[^>]*>");
    Pattern latinTextRegex = Pattern.compile("^[#±§-‒–—―©®™½¾@€£$¥&\u20BD\u00A0\u00AD%\\[\\])(（）;:,\\..?!\"'×Þß÷þøA-zÀ-ÿćęłńóśźżĄĆĘŁŃÓŚŹŻ0-9\\s-\\t/+α-ωΑ-Ω-]+$");
    Pattern someLettersRegex = Pattern.compile("[A-z]+");

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        context.write(new Text("# parser = MaltParser Language: English. Parser configuration: Stack. Transition system: Projective. Model: de.tudarmstadt.ukp.dkpro.core.maltparser-upstream-parser-en-linear. Model version: 20120312."), NullWritable.get());

        parserName = context.getConfiguration().getStrings("parserName", "malt")[0];
        log.info("Parser ('malt' or 'stanford'): " + parserName);

        collapsing = context.getConfiguration().getBoolean("collapsing", true);
        log.info("Collapse dependencies ('true' or 'false'): " + collapsing);

        inputType = context.getConfiguration().getStrings("inputType", SENTENCE)[0].toLowerCase();
        if (!inputType.equals(DOCUMENT) && !inputType.equals(SENTENCE)) inputType = SENTENCE;
        log.info("Input type ('sentence' or 'document'): " + inputType);

        try {
            segmenter = AnalysisEngineFactory.createEngine(StanfordSegmenter.class);
            posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
            lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
            if (parserName.toLowerCase().contains("malt")) {
                synchronized (MaltParser.class) {
                    parser = AnalysisEngineFactory.createEngine(MaltParser.class);
                }
            }
            else if (parserName.toLowerCase().contains("stanford")) {
                parser = AnalysisEngineFactory.createEngine(StanfordParser.class,
                        StanfordParser.PARAM_VARIANT, "pcfg",
                        StanfordParser.PARAM_LANGUAGE, "en");
            }
            else {
                synchronized (MaltParser.class) {
                    parser = AnalysisEngineFactory.createEngine(MaltParser.class);
                }
            }
            if (collapsing){
                String rulesPath = extractRulesFile();
                collapser = AnalysisEngineFactory.createEngine(
                        CollapsedDependenciesAnnotator.class,
                        CollapsedDependenciesAnnotator.RULE_MANAGER, rulesPath);
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

    private String extractRulesFile() throws IOException {
        InputStream inStream = getClass().getClassLoader().getResourceAsStream("data/collapsing_rules_english_cc.txt");
        File rulesPath = new File("./rules.txt");
        if (!Files.exists(rulesPath.toPath())) {
            OutputStream outStream = new FileOutputStream(rulesPath);
            IOUtils.copy(inStream, outStream);
            inStream.close();
            outStream.close();
        }
        return rulesPath.getAbsolutePath();
    }

    public String cleanup(String document, Context context) {
        try {
            document = Jsoup.parse(document.replace("   ", " . ")).text();
            jCas.reset();
            jCas.setDocumentText(document);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);
            StringBuilder d = new StringBuilder();

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());
                if (tokens.size() > maxSentenceSizeTokens) {
                    context.getCounter("de.tudarmstadt.lt.wsi", "NUM_SKIPPED_SENTENCES").increment(1);
                    continue;
                }

                String s = sentence.getCoveredText();
                Matcher urlMatch = urlRegex.matcher(s);
                Matcher htmlMatch = htmlRegex.matcher(s);
                Matcher latinMatch = latinTextRegex.matcher(s);
                Matcher lettersMatch = someLettersRegex.matcher(s);

                if((!urlMatch.find() && !htmlMatch.find()) && (latinMatch.find() && lettersMatch.find())){
                    d.append(sentence.getCoveredText().replaceAll("\\s+", " "));
                    d.append(" ");
                }
            }
            return d.toString();
        } catch(Exception e){
            if(verbose) log.error("Can't process document.", e);
            return "";
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
            String url = "";
            String s3 = "";
            String text = "";
            if (inputType.equals(SENTENCE)){
                text = line.toString();
            } else {
                String[] fields = line.toString().split("\t");
                if (fields.length == 3){
                    url = fields[0];
                    s3 = fields[1];
                    text = cleanup(fields[2], context);
                }
            }

            jCas.reset();
            jCas.setDocumentText(text);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);
            posTagger.process(jCas);
            lemmatizer.process(jCas);
            nerEngine.process(jCas);
            parser.process(jCas);
            if (collapsing) collapser.process(jCas);

            // For each dependency output a field with ten columns ending with the bio named entity: http://universaldependencies.org/docs/format.html
            // IN_ID TOKEN LEMMA POS_COARSE POS_FULL MORPH ID_OUT TYPE _ NE_BIO
            // An example is below. NB: the last (10th) field can be anything according to the specification (NE in our case)
            // 5 books book NOUN NNS Number=Plur 2 dobj 4:dobj SpaceAfter=No

            if (inputType.equals(DOCUMENT)){
                context.write(new Text("\n# newdoc\turl = " + url + "\ts3 = " + s3), NullWritable.get());
                context.getCounter("de.tudarmstadt.lt.wsi", "NUM_PROCESSED_DOCUMENTS").increment(1);
            }

            int sentenceId = 1;
            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());
                if (tokens.size() > maxSentenceSizeTokens) {
                    context.getCounter("de.tudarmstadt.lt.wsi", "NUM_SKIPPED_SENTENCES_2").increment(1);
                    continue;
                }

                HashMap<Token, Integer> tokenToID = collectionToMap(tokens);
                List<NamedEntity> ngrams = JCasUtil.selectCovered(jCas, NamedEntity.class, sentence);
                context.write(new Text("\n# sent_id = " + url + "#" + sentenceId), NullWritable.get());
                context.write(new Text("# text = " + sentence.getCoveredText()), NullWritable.get());

                HashMap<Token, String> tokenToCDep = new HashMap<>();
                if (collapsing) {
                    for (NewCollapsedDependency dep : JCasUtil.selectCovered(jCas, NewCollapsedDependency.class, sentence.getBegin(), sentence.getEnd())) {
                        if (dep instanceof NewCollapsedDependency) {
                            tokenToCDep.put(dep.getDependent(), tokenToID.getOrDefault(dep.getGovernor(), -1) + ":" + dep.getDependencyType());
                        }
                    }
                }

                TreeMap<Integer, Line> conllLines = new TreeMap<>();
                for (Dependency dep : JCasUtil.selectCovered(jCas, Dependency.class, sentence.getBegin(), sentence.getEnd())) {
                    if (dep instanceof NewCollapsedDependency) continue;
                    Integer idSrc = tokenToID.getOrDefault(dep.getDependent(), -2);
                    Line l = new Line(
                            idSrc,
                            dep.getDependent().getCoveredText(),
                            dep.getDependent().getLemma() != null ? dep.getDependent().getLemma().getValue() : "",
                            dep.getDependent().getPos() != null ? dep.getDependent().getPos().getPosValue() : "",
                            dep.getDependent().getPos() != null ? dep.getDependent().getPos().getPosValue() : "",
                            dep.getDependent().getMorph() != null ? dep.getDependent().getMorph().getValue() : "",
                            tokenToID.getOrDefault(dep.getGovernor(), -2),
                            dep.getDependencyType(),
                            tokenToCDep.getOrDefault(dep.getDependent(),"_"),
                            getBIO(ngrams, dep.getBegin(), dep.getEnd())
                    );
                    conllLines.put(idSrc, l);
                }

                for (Integer id : conllLines.keySet()) {
                    String res = String.format("%d\t%s", id, conllLines.get(id).getTextNoId());
                    context.write(new Text(res), NullWritable.get());
                }

                context.getCounter("de.tudarmstadt.lt.wsi", "NUM_PROCESSED_SENTENCES").increment(1);
                sentenceId += 1;
            }
        } catch(Exception e){
            if (verbose) log.error("Can't process line: " + line.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }

    private HashMap<Token, Integer> collectionToMap(Collection<Token> tokens){
        HashMap<Token,Integer> token2id = new HashMap<>();
        Integer id = 0;
        for (Token t : tokens){
            token2id.put(t, id);
            id++;
        }
        return token2id;
    }
}

