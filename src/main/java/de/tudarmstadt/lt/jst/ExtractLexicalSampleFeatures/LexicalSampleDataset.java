package de.tudarmstadt.lt.jst.ExtractLexicalSampleFeatures;

import de.tudarmstadt.lt.jst.Const;
import de.tudarmstadt.lt.jst.Utils.Format;

import java.util.List;

public class LexicalSampleDataset {
    /**
     * Parses stores, and provides access to a WSD lexical sample dataset line in the format (tab separated):
     * "context_id  target  target_pos  target_position gold_sense_ids  predict_sense_ids   golden_related  predict_related context"
     * e.g.:
     * '2 add.v.1 add v 51,57 add%2:32:01::/4  Lewinsky wrote "Return to Sender" on the envelope, adding, "You must be morons to send me this letter!"'
     * */

    public static final String HEADER_9_COLS = "context_id\ttarget\ttarget_pos\ttarget_position\tgold_sense_ids\tpredict_sense_ids\tgolden_related\tpredict_related\tcontext";
    public static final String HEADER_SUPPLIMENT_FEATURES = "\tword_features\tholing_features\ttarget_holing_features";

    public String context_id;
    public String target;
    public String target_pos;
    public String target_position;
    public String gold_sense_ids;
    public String predict_sense_ids;
    public String golden_related;
    public String predict_related;
    public String context;
    public String word_features;
    public String holing_features;
    public String target_holing_features;

    public LexicalSampleDataset(String line) {
        String[] fields = line.split("\t");
        if (fields.length < 9) {
            System.out.println("Warning: wrong lexical sample line '" + line + "'");
            return;
        }
        context_id = fields[0];
        target = fields[1];
        target_pos = fields[2];
        target_position = fields[3];
        gold_sense_ids = fields[4];
        predict_sense_ids = fields[5];
        golden_related = fields[6];
        predict_related = fields[7];
        context = fields[8];
        word_features = "";
        holing_features = "";
        target_holing_features = "";
    }

    public String getHeader(boolean withFeatures){
        if (withFeatures) return HEADER_9_COLS + HEADER_SUPPLIMENT_FEATURES;
        else return HEADER_9_COLS;
    }

    public void setFeatures(List<String> wordFeatures, List<String> holingFeatures, List<String> targetHolingFeatures) {
        word_features = Format.join(wordFeatures, Const.LIST_SEP);
        holing_features = Format.join(holingFeatures, Const.LIST_SEP);
        target_holing_features = Format.join(targetHolingFeatures, Const.LIST_SEP);
    }

    public String asString() {
        return
            context_id + "\t" +
            target + "\t" +
            target_pos + "\t" +
            target_position + "\t" +
            gold_sense_ids + "\t" +
            predict_sense_ids  + "\t" +
            golden_related + "\t" +
            predict_related + "\t" +
            context + "\t" +
            word_features + "\t" +
            holing_features + "\t" +
            target_holing_features;
    }
}
