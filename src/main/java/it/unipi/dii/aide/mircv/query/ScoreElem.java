package it.unipi.dii.aide.mircv.query;

public class ScoreElem {
    private int index;
    private double score;

    public ScoreElem(int index, double score) {
        this.index = index;
        this.score = score;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
