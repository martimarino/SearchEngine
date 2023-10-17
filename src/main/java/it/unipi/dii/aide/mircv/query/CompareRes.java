package it.unipi.dii.aide.mircv.query;

import java.util.Comparator;
//order in increasing order 
public class CompareRes implements Comparator<ResultBlock> {
    @Override
    public int compare(ResultBlock pb1, ResultBlock pb2) {

        int scoreCompare = Double.compare(pb1.getScore(), pb2.getScore());

        if (scoreCompare == 0)
            return Integer.compare(pb1.getDocId(), pb2.getDocId());

        return scoreCompare;
    }
}
