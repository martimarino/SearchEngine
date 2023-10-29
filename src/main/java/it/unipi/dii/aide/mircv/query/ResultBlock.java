package it.unipi.dii.aide.mircv.query;

import java.util.Comparator;

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


    public static class CompareRes implements Comparator<ResultBlock> {
        @Override
        public int compare(ResultBlock pb1, ResultBlock pb2) {

            int scoreCompare = Double.compare(pb1.getScore(), pb2.getScore());

            if (scoreCompare == 0)
                return Integer.compare(pb1.getDocId(), pb2.getDocId());

            return scoreCompare;
        }
    }

    public static class CompareResInverse implements Comparator<ResultBlock> {
        @Override
        public int compare(ResultBlock pb1, ResultBlock pb2) {

            int scoreCompare = Double.compare(pb2.getScore(), pb1.getScore());

            if (scoreCompare == 0)
                return Integer.compare(pb1.getDocId(), pb2.getDocId());

            return scoreCompare;
        }
    }
}

