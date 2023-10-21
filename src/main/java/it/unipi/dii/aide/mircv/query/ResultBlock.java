package it.unipi.dii.aide.mircv.query;

public class ResultBlock {
    private int docId;
    private double score;

    // constructor with parameters
    public ResultBlock(int docId, double score) {
        this.docId = docId;
        this.score = score;
    }

    public int getDocId() {
        return docId;
    }

    public double getScore() {
        return score;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public void setScore(double score) {
        this.score = score;
    }


    @Override
    public String toString() {
        return "ResultBlock{" +
                "docId=" + docId +
                ", score=" + score +
                '}';
    }
}

