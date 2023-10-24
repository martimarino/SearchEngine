package it.unipi.dii.aide.mircv.query.scores;

import java.util.Comparator;

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

    /**
     * Orders the ScoreElem in increasing order of score and, in case of equal score, in increasing order of index
     */
    public static class CompareScoreElem implements Comparator<ScoreElem> {

        @Override
        public int compare(ScoreElem o1, ScoreElem o2) {
            int score = Double.compare(o1.getScore(), o2.getScore());
            // if same score, order by index
            if (score == 0) {
                return Double.compare(o1.getIndex(), o2.getIndex());
            }

            return score;
        }
    }
}
