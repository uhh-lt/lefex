[![Build Status](https://travis-ci.org/tudarmstadt-lt/lefex.svg?branch=master)](https://travis-ci.org/tudarmstadt-lt/lefex) [![Release](https://jitpack.io/v/tudarmstadt-lt/lefex.svg)](https://jitpack.io/#tudarmstadt-lt/lefex)

# lefex: A Tool for LExical FEature eXtraction

This project contains Hadoop jobs for extraction of features of words and texts. Currently, the following types of features can be extracted:

1. **CoNLL**. Given a set of HTML documents in the CSV format ```url<TAB>s3-path<TAB>html-document``` and outputs the dependency parsed documents in the [CoNLL format](http://universaldependencies.org/format.html). See the ```de.uhh.lt.lefex.CoNLL.HadoopMain``` class.
2. **ExtractTermFeatureScores**. Given a corpus in plain text format, extract word count (```word<TAB>count```), feature count (```feature<TAB>count```), and word-feature count (```word<TAB>feature<TAB>count```) and save these into CSV files. See the ```de.uhh.lt.lefex.ExtractTermFeatureScores.HadoopMain``` class.
3. **ExtractLexicalSampleFeatureScores**. Given a lexical sample dataset for word sense disambiguation in CSV format, extract features of the target word in context and add them as an extra column.  Currently, the system supports extraction of three types of features of a target word: 
co-occurrences, dependency features, and trigrams. See the ```de.uhh.lt.lefex.ExtractLexicalSampleFeatures.HadoopMain``` class. 
4. **SentenceSplitter**. This job take a plain text corpus as an input and outputs a file with exactly one sentence per line. See the ```de.uhh.lt.lefex.SentenceSplitter.HadoopMain``` class. 
This project is used for feature extraction in the [JoSimText project](https://github.com/uhh-lt/JoSimText).
