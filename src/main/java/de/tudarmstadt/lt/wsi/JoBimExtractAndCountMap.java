package de.tudarmstadt.lt.wsi;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
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
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;


class JoBimExtractAndCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
	AnalysisEngine engine;
	JCas jCas;
	
	private static IntWritable ONE = new IntWritable(1);

	public AnalysisEngineDescription buildAnalysisEngine() throws ResourceInitializationException {
		AnalysisEngineDescription segmenter = AnalysisEngineFactory.createEngineDescription(OpenNlpSegmenter.class);
		AnalysisEngineDescription pos = AnalysisEngineFactory.createEngineDescription(OpenNlpPosTagger.class);
		AnalysisEngineDescription lemmatizer = AnalysisEngineFactory.createEngineDescription(StanfordLemmatizer.class);
		AnalysisEngineDescription deps = AnalysisEngineFactory.createEngineDescription(MaltParser.class);

		return AnalysisEngineFactory.createEngineDescription(
				segmenter, pos, lemmatizer, deps);
	}
	
	@Override
	public void setup(Context context) {
		log.info("Initializing JoBimExtractAndCount...");
		try {
			engine = AnalysisEngineFactory.createEngine(buildAnalysisEngine());
			jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
		} catch (ResourceInitializationException e) {
			log.error("Couldn't initialize analysis engine", e);
		} catch (CASException e) {
			log.error("Couldn't create new CAS", e);
		}
		log.info("Ready!");
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		try {
			String text = value.toString();
			jCas.reset();
			jCas.setDocumentText(text);
			jCas.setDocumentLanguage("en");
			engine.process(jCas);
			Collection<Token> tokens = JCasUtil.select(jCas, Token.class);
			for (Token token : tokens) {
				String pos = token.getPos().getPosValue();
				String lemma = token.getLemma().getValue();
				context.write(new Text("W\t" + lemma), ONE);
				if (pos.equals("NN") || pos.equals("NNS")) {
					for (Token token2 : tokens) {
						if (!token2.equals(token)) {
							String lemma2 = token2.getLemma().getValue();
							context.write(new Text("COOC-WF\t" + lemma + "\t" + lemma2), ONE);
						}
					}
				}
			}
			Collection<Dependency> deps = JCasUtil.select(jCas, Dependency.class);
			for (Dependency dep : deps) {
				Token source = dep.getGovernor();
				Token target = dep.getDependent();
				String rel = dep.getDependencyType();
				String sourcePos = source.getPos().getPosValue();
				String targetPos = target.getPos().getPosValue();
				String sourceLemma = source.getLemma().getValue();
				String targetLemma = target.getLemma().getValue();
				if (sourcePos.equals("NN") || sourcePos.equals("NNS")) {
					context.write(new Text("DEP-F\t(@@," + targetLemma + ")"), ONE);
					context.write(new Text("DEP-WF\t" + sourceLemma + "\t" + rel + "(@@," + targetLemma + ")"), ONE);
				}
				if (targetPos.equals("NN") || targetPos.equals("NNS")) {
					context.write(new Text("DEP-F\t(" + sourceLemma + ",@@)"), ONE);
					context.write(new Text("DEP-WF\t" + targetLemma + "\t" + rel + "(" + sourceLemma + ",@@)"), ONE);
				}
			}
		} catch (Exception e) {
			log.error("Can't process line: " + value.toString(), e);
			context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
		}
	}
}
