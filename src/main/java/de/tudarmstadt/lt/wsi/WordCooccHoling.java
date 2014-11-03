package de.tudarmstadt.lt.wsi;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordLemmatizer;

public class WordCooccHoling extends Configured implements Tool {
	private static class WordCooccHolingMap extends Mapper<LongWritable, Text, Text, NullWritable> {
		Logger log = Logger.getLogger("de.tudarmstadt.lt.wiki");
		AnalysisEngine engine;
		JCas jCas;
		String inputSplit;

		public AnalysisEngineDescription buildAnalysisEngine() throws ResourceInitializationException {
			AnalysisEngineDescription segmenter = AnalysisEngineFactory.createEngineDescription(OpenNlpSegmenter.class);
			AnalysisEngineDescription pos = AnalysisEngineFactory.createEngineDescription(OpenNlpPosTagger.class);
			AnalysisEngineDescription lemmatizer = AnalysisEngineFactory.createEngineDescription(StanfordLemmatizer.class);

			return AnalysisEngineFactory.createEngineDescription(
					segmenter, pos, lemmatizer);
		}
		
		@Override
		public void setup(Context context) {
			log.info("Initializing WordDependencyHoling...");
			try {
				engine = AnalysisEngineFactory.createEngine(buildAnalysisEngine());
				jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
			} catch (ResourceInitializationException e) {
				log.error("Couldn't initialize analysis engine", e);
			} catch (CASException e) {
				log.error("Couldn't create new CAS", e);
			}
			log.info("Ready!");
			inputSplit = context.getInputSplit().toString();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			try {
				String text = value.toString();
				String dataset = inputSplit + "/" + key.toString();
//				log.info("Handling sentence of length " + text.length());
				jCas.reset();
				jCas.setDocumentText(text);
				jCas.setDocumentLanguage("en");
				engine.process(jCas);
				Collection<Token> tokens = JCasUtil.select(jCas, Token.class);
				for (Token token : tokens) {
					String pos = token.getPos().getPosValue();
					String lemma = token.getLemma().getValue();
					String tokenSpan = token.getBegin() + ":" + token.getEnd();
					if (pos.equals("NN") || pos.equals("NNS")) {
						for (Token token2 : tokens) {
							String lemma2 = token2.getLemma().getValue();
							String token2Span = token2.getBegin() + ":" + token2.getEnd();
							context.write(new Text(lemma + "\t" + lemma2 + "\t" + dataset + "\t" + tokenSpan + "\t" + token2Span), NullWritable.get()); 
						}
					}
				}
			} catch (Exception e) {
				log.error("Can't process line: " + value.toString(), e);
				context.getCounter("de.tudarmstadt.lt.wiki", "NUM_MAP_ERRORS").increment(1);
			}
		}
	}

	public boolean runJob(String inDir, String outDir) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		String _outDir = outDir;
		int outDirSuffix = 1;
		while (fs.exists(new Path(_outDir))) {
			_outDir = outDir + outDirSuffix;
			outDirSuffix++;
		}
		conf.setBoolean("mapred.output.compress", true);
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCooccHoling.class);
		FileInputFormat.addInputPath(job, new Path(inDir));
		FileOutputFormat.setOutputPath(job, new Path(_outDir));
		job.setMapperClass(WordCooccHolingMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		job.setJobName("NounSenseInduction:WordCooccHoling");
		return job.waitForCompletion(true);
	}

	public int run(String[] args) throws Exception {
		System.out.println("args:" + Arrays.asList(args));
		if (args.length != 2) {
			System.out.println("Usage: </path/to/wiki-links> </path/to/output>");
			System.exit(1);
		}
		String inDir = args[0];
		String outDir = args[1];
		boolean success = runJob(inDir, outDir);
		return success ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new WordCooccHoling(), args);
		System.exit(res);
	}
}