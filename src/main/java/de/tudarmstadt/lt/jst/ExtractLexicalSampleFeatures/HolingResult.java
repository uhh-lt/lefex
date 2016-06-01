package de.tudarmstadt.lt.jst.ExtractLexicalSampleFeatures;

import java.util.List;

public class HolingResult {
    public HolingResult(List<String> allFeatures, List<String> targetFeatures){
        this.allFeatures = allFeatures;
        this.targetFeatures = targetFeatures;
    }

    public List<String> allFeatures;
    public List<String> targetFeatures;
}
