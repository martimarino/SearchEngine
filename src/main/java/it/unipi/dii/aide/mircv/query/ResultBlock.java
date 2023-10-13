package it.unipi.dii.aide.mircv.query;

public class ResultBlock {

    private String docNo;
    private int docId;
    private double score;

    // constructor with parameters
    public ResultBlock(String docNo, int docId, double score) {
        this.docNo = docNo;
        this.docId = docId;
        this.score = score;
    }

    public int getDocId() {
        return docId;
    }

    public double getScore() {
        return score;
    }

    public String getDocNo() {
        return docNo;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
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
                "docNo=" + docNo +
                ", docId=" + docId +
                ", score=" + score +
                '}';
    }

}

