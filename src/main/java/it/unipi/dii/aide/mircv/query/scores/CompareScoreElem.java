package it.unipi.dii.aide.mircv.query.scores;

import it.unipi.dii.aide.mircv.query.scores.ScoreElem;

import java.util.Comparator;

/**
 * Orders the ScoreElem in increasing order of score and, in case of equal score, in increasing order of index
 */
public class CompareScoreElem implements Comparator<ScoreElem> {

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
