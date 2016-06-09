package de.tudarmstadt.lt.jst.SentenceSplitter;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
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

import java.io.IOException;
import java.util.Collection;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

class HadoopMap extends Mapper<LongWritable, Text, LongWritable, Text> {
    Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
	AnalysisEngine segmenter;
	JCas jCas;
    int maxSentenceSize = 150;  // tokens

	@Override
	public void setup(Context context) {
        maxSentenceSize = context.getConfiguration().getInt("max_sentences_size", 150);
        try {
            segmenter = AnalysisEngineFactory.createEngine(OpenNlpSegmenter.class);
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
		try {
            String text = value.toString();
            context.getCounter("de.tudarmstadt.lt", "TOTAL_LINES").increment(1);
            jCas.reset();
            jCas.setDocumentText(text);
            jCas.setDocumentLanguage("en");
            segmenter.process(jCas);

            for (Sentence sentence : JCasUtil.select(jCas, Sentence.class)) {
                Collection<Token> tokens = JCasUtil.selectCovered(jCas, Token.class, sentence.getBegin(), sentence.getEnd());
                context.getCounter("de.tudarmstadt.lt", "TOTAL_SENTENCES").increment(1);
                if(tokens.size() <= maxSentenceSize) {
                    context.write(key, new Text(text.substring(sentence.getBegin(), sentence.getEnd())));
                    context.getCounter("de.tudarmstadt.lt", "SENTENCES_WRITTEN").increment(1);
                } else {
                    context.getCounter("de.tudarmstadt.lt", "SENTENCES_SKIPPED").increment(1);
                }
            }

        } catch(Exception e){
            log.error("Can't process line: " + value.toString(), e);
            context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
        }
    }
}
