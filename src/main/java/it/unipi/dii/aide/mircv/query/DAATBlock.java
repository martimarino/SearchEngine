package it.unipi.dii.aide.mircv.query;

import java.util.Comparator;

/**
 * class to define DAATBlock. The priority queue contains instances of DAATBlock
 */
public class DAATBlock {

    private String term;
    private int docId;                  // DocID
    private double score;     // reference to the posting list (index in the array of posting lists of the query) containing DcoID

    // constructor with parameters
    public DAATBlock(String term, int docId, double score) {
        this.term = term;
        this.docId = docId;
        this.score = score;
    }

    public int getDocId() {
        return docId;
    }

    public double getScore() {
        return score;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "DAATBlock{" +
                "term='" + term + '\'' +
                ", docId=" + docId +
                ", score=" + score +
                '}';
    }

}

