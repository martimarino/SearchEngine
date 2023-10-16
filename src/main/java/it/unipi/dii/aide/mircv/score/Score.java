package it.unipi.dii.aide.mircv.score;

import it.unipi.dii.aide.mircv.Query;
import it.unipi.dii.aide.mircv.data_structures.CollectionStatistics;
import it.unipi.dii.aide.mircv.data_structures.IndexMerger;
import it.unipi.dii.aide.mircv.data_structures.Posting;

import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.Query.documentTable;

public final class Score {

    private Score() {
        throw new UnsupportedOperationException();
    }

/*
    public static double computeTFIDF(String term, Posting p) {

        double tf = 1 + Math.log10(p.getTermFreq());
        double idf = Query.dictionary.getTermToTermStat().get(term).getIdf();

        return tf*idf;
    }
*/

    public static double computeTFIDF(Double idf, Posting p) {

        double tf = 1 + Math.log10(p.getTermFreq());
        return tf*idf;
    }


    public static double computeBM25(Double idf, Posting p){
        return (p.getTermFreq()/
                ((1-0.75) + 0.75*(IndexMerger.documentTable.get(p.getDocId()).getDoclength() /
                        (CollectionStatistics.getTotDocLen()/CollectionStatistics.getNDocs()))
                        + p.getTermFreq()))*idf;
    }

    public static double computeMaxBM25(ArrayList<Posting> postings, double idf){
        double score = 0;
        double maxScore = 0;
        for(Posting p : postings)
        {
            score = computeBM25(idf, p);
            if(score > maxScore)
                maxScore = score;
        }
        return maxScore;
    }

    public static double computeMaxTFIDF(ArrayList<Posting> postings, double idf){

        double score = 0;
        double maxScore = 0;
        for(Posting p : postings)
        {
            score = computeTFIDF(idf, p);
            if(score > maxScore)
                maxScore = score;
        }
        return maxScore;

    }
}
