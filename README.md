[![Build Status](https://travis-ci.org/tudarmstadt-lt/lefex.svg?branch=master)](https://travis-ci.org/tudarmstadt-lt/lefex) [![Release](https://jitpack.io/v/tudarmstadt-lt/lefex.svg)](https://jitpack.io/#tudarmstadt-lt/lefex)

# LEFEX: Lexical Feature Extraction

This project contains Hadoop jobs for extraction of features of words and texts. Currently, the following types of features can be extracted:

1. Given a corpus, extract word count (W), feature count (F), and word-feature count (WF). 
2. Given a lexical sample dataset for word sense disambiguation, extract features of the target word in context. 

Currently, the system supports extraction of three types of features of a target word: 

- co-occurrences
- dependency features
- trigrams

This project is used for feature extraction in the JoSimText project: https://github.com/tudarmstadt-lt/JoSimText
