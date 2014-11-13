package de.tudarmstadt.lt.wsi;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;


class JoBimExtractAndCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	Logger log = Logger.getLogger("de.tudarmstadt.lt.wsi");
	AnalysisEngine segmenter;
	AnalysisEngine posTagger;
	AnalysisEngine lemmatizer;
	AnalysisEngine depParser;
	JCas jCas;
	boolean semantifyDependencies;
	boolean computeDependencies;
	boolean computeCoocs;
	
	private static IntWritable ONE = new IntWritable(1);
	
	@Override
	public void setup(Context context) {
		log.info("Initializing JoBimExtractAndCount...");
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
		computeCoocs = context.getConfiguration().getBoolean("holing.coocs", false);
		computeDependencies = context.getConfiguration().getBoolean("holing.dependencies", false);
		semantifyDependencies = context.getConfiguration().getBoolean("holing.dependencies.semantify", false);
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
			segmenter.process(jCas);
			Collection<Token> tokens = JCasUtil.select(jCas, Token.class);
			Set<String> tokenSet = new HashSet<String>();
			final int MAX_NUM_TOKENS = 100;
			if (tokens.size() > MAX_NUM_TOKENS) {
				context.getCounter("de.tudarmstadt.lt.wsi", "NUM_SKIPPED_SENTENCES").increment(1);
				return;
			}

			posTagger.process(jCas);
			lemmatizer.process(jCas);
			
			for (Token token : tokens) {
				String lemma = token.getLemma().getValue();
				context.write(new Text("W\t" + lemma), ONE);
			}
			
			if (computeCoocs) {
				for (Token token : tokens) {
					String lemma = token.getLemma().getValue();
					tokenSet.add(lemma);
				}
				
				for (String lemma : tokenSet) {
					context.write(new Text("CoocF\t" + lemma), ONE);
				}
				
				for (Token token : tokens) {
					String pos = token.getPos().getPosValue();
					String lemma = token.getLemma().getValue();
					if (pos.equals("NN") || pos.equals("NNS")) {
						for (String lemma2 : tokenSet) {
							context.write(new Text("CoocWF\t" + lemma + "\t" + lemma2), ONE);
						}
					}
					context.progress();
				}
			}
			
			if (computeDependencies) {
				depParser.process(jCas);
				Collection<Dependency> deps = JCasUtil.select(jCas, Dependency.class);
				for (Dependency dep : deps) {
					Token source = dep.getGovernor();
					Token target = dep.getDependent();
					String rel = dep.getDependencyType();
					if (semantifyDependencies) {
						rel = semantifyDependencyRelation(rel);
						if (rel == null) {
							continue;
						}
					}
					String sourcePos = source.getPos().getPosValue();
					String targetPos = target.getPos().getPosValue();
					String sourceLemma = source.getLemma().getValue();
					String targetLemma = target.getLemma().getValue();
					if (sourcePos.equals("NN") || sourcePos.equals("NNS")) {
						String bim = rel + "(@@," + targetLemma + ")";
						context.write(new Text("DepF\t" + bim), ONE);
						context.write(new Text("DepWF\t" + sourceLemma + "\t" + bim), ONE);
					}
					if (targetPos.equals("NN") || targetPos.equals("NNS")) {
						String bim = rel + "(" + sourceLemma + ",@@)";
						context.write(new Text("DepF\t" + bim), ONE);
						context.write(new Text("DepWF\t" + targetLemma + "\t" + bim), ONE);
					}
					context.progress();
					
				}
			}
		} catch (Exception e) {
			log.error("Can't process line: " + value.toString(), e);
			context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
		}
	}
	
	private String semantifyDependencyRelation(String rel) {
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
		return null;
	}
}
