package senseval2007;

import static org.apache.uima.fit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasCreationUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import de.tudarmstadt.lt.wsi.StanfordLemmatizer;
import de.tudarmstadt.lt.wsi.Util;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import de.tudarmstadt.ukp.dkpro.core.api.syntax.type.dependency.Dependency;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import edu.stanford.nlp.util.StringUtils;

public class SensEvalRunner extends DefaultHandler {
	StringBuffer buf;
	String lexeltId;
	JCas jCas;
	AnalysisEngine posTagger;
	AnalysisEngine lemmatizer;
	AnalysisEngine depParser;
	Writer writer;
	
	public SensEvalRunner(String outputFile) throws CASException, ResourceInitializationException, IOException {
		posTagger = AnalysisEngineFactory.createEngine(OpenNlpPosTagger.class);
		lemmatizer = AnalysisEngineFactory.createEngine(StanfordLemmatizer.class);
		synchronized(MaltParser.class) {
			depParser = AnalysisEngineFactory.createEngine(MaltParser.class);
		}
		jCas = CasCreationUtils.createCas(createTypeSystemDescription(), null, null).getJCas();
		writer = new FileWriter(outputFile);
	}
	
	@Override
    public void startElement (String uri, String localName,
                               String qName, Attributes atts) throws SAXException {
		if ("instance".equals(qName)) {
			System.out.println(atts.getValue("id"));
			buf = new StringBuffer();
			lexeltId = atts.getValue("id");
		}
		else if ("head".equals(qName)) {
			buf.append("<head>");
		}
    }
	
	@Override
    public void endElement (String uri, String localName,
                               String qName) throws SAXException {
		if ("instance".equals(qName)) {
			jCas.reset();
			jCas.setDocumentLanguage("en");
			String rawText = buf.toString().replace("\n", "");
			String[] tokensArr = rawText.split(" ");
			Token head = null;
			int pos = 0;
			int sentPos = 0;
			ArrayList<String> documentTokens = new ArrayList<String>(tokensArr.length - 2);
			Token lastToken = null;
			Sentence s = new Sentence(jCas);
			//Sentence headS = null;
			for (int i = 0; i < tokensArr.length; i++) {
				String token = tokensArr[i];
				if ("<head>".equals(token)) {
					continue;
				}
				if ("</head>".equals(token)) {
					head = lastToken;
					//headS = s;
					continue;
				}
				documentTokens.add(token);
				if (".".equals(token)) {
					s.setBegin(sentPos);
					s.setEnd(pos);
					s.addToIndexes();
					sentPos = pos;
					s = new Sentence(jCas);
				}
				Token t = new Token(jCas, pos, pos + token.length());
				t.addToIndexes();
				lastToken = t;
				pos += token.length() + 1;
			}
			// Set CAS text
			jCas.setDocumentText(StringUtils.join(documentTokens, " "));
			
			try {
				posTagger.process(jCas);
				lemmatizer.process(jCas);
				depParser.process(jCas);
			} catch (AnalysisEngineProcessException e) {
				e.printStackTrace();
			}

			Collection<Token> tokens = JCasUtil.select(jCas, Token.class);
			//Collection<POS> posTags = JCasUtil.select(jCas, POS.class);
			Collection<Lemma> lemmas = JCasUtil.select(jCas, Lemma.class);
			Collection<Dependency> deps = JCasUtil.select(jCas, Dependency.class);

			Collection<Dependency> depsCollapsed = Util.collapseDependencies(jCas, deps, tokens);
			ArrayList<String> depFeatures = new ArrayList<String>();
			for (Dependency dep : depsCollapsed) {
				Token source = dep.getGovernor();
				Token target = dep.getDependent();
				if (source == head) {
					String rel = dep.getDependencyType();
					String targetLemma = target.getLemma().getValue();
					String bim = rel + "(@@," + targetLemma + ")";
					depFeatures.add(bim);
				}
				if (target == head) {
					String rel = dep.getDependencyType();
					String sourceLemma = source.getLemma().getValue();
					String bim = rel + "(" + sourceLemma + ",@@)";
					depFeatures.add(bim);
				}
			}
			
			ArrayList<String> coocFeatures = new ArrayList<String>();
			for (Lemma lemma : lemmas) {
				coocFeatures.add(lemma.getValue());
			}
			
			String lexeltLemma = lexeltId.split("\\.")[0];
			String line = lexeltLemma + "\t" + lexeltId + "\t" + StringUtils.join(coocFeatures, " ") + "\t" + StringUtils.join(depFeatures, " ");
			try {
				writer.write(line + "\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if ("head".equals(qName)) {
			buf.append("</head>");
		}
    }
	
	@Override
    public void characters (char ch[], int start, int length) throws SAXException {
		if (buf != null) {
			buf.append(ch, start, length);
		}
	}
	
	@Override
	public void endDocument() throws SAXException {
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		String instanceFile = args[0]; // "/Users/jsimon/MA/SemEval2007Task2/key/data/English_sense_induction2.xml"
		//Reader modelFileReader = new FileReader(args[1]);
		String outputFile = args[1]; // "/Users/jsimon/MA/SemEval2007Task2/key/data/English_sense_induction.txt"
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		//Map<String, List<Cluster<String>>> model = ClusterReaderWriter.readClusters(modelFileReader);
		SAXParser saxParser = spf.newSAXParser();
		SensEvalRunner xmlHandler = new SensEvalRunner(outputFile);
		saxParser.parse(new File(instanceFile), xmlHandler);
	}
}