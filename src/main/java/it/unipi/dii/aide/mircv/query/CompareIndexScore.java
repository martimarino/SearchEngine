package it.unipi.dii.aide.mircv.query;

import java.util.Comparator;

public class CompareIndexScore implements Comparator<ScoreElem> {

    @Override
    public int compare(ScoreElem o1, ScoreElem o2) {
        int score = Double.compare(o1.getScore(), o2.getScore());
        // if the DocID are equal, compare by block number
        if (score == 0) {
            return Double.compare(o1.getIndex(), o2.getIndex());
        }

        return score;
    }
}
