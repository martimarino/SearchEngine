package it.unipi.dii.aide.mircv.score;

import it.unipi.dii.aide.mircv.Query;
import it.unipi.dii.aide.mircv.data_structures.Posting;

public final class Score {

    private Score() {
        throw new UnsupportedOperationException();
    }

    public static double computeTFIDF(String term, Posting p) {

        double tf = 1 + Math.log10(p.getTermFreq());
        double idf = Query.dictionary.getTermToTermStat().get(term).computeIdf();

        return tf*idf;
    }


}