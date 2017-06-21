package de.tudarmstadt.lt.jst.CoNLL;


public class Line {
    public Line(int idSrc, String token, String lemma, String pos, String posFull, String morph, int idDst, String depType, String enhancedDepType, String bio){
        this.idSrc = idSrc;
        this.token = token;
        this.lemma = lemma;
        this.pos = pos;
        this.posFull = posFull;
        this.morph = morph;
        this.idDst = idDst;
        this.depType = depType;
        this.enhancedDepType = enhancedDepType;
        this.bio = bio;
    }

    int idSrc;
    String token;
    String lemma;
    String pos;
    String posFull;
    String morph;
    int idDst;
    String depType;
    String enhancedDepType;
    String bio;

    public String getText(){
        return String.format("%d\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s",
                idSrc, token, lemma, pos, posFull, morph, idDst, depType, enhancedDepType, bio);
    }

    public String getTextNoId(){
        return String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s",
                token, lemma, pos, posFull, morph, idDst, depType, enhancedDepType, bio);
    }
}
