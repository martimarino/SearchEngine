package it.unipi.dii.aide.mircv.query.algorithms;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.query.PostingList;
import it.unipi.dii.aide.mircv.query.ResultBlock;

import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.dictionary;
import static it.unipi.dii.aide.mircv.query.Query.*;
import static it.unipi.dii.aide.mircv.utils.Constants.printDebug;

public class Conjunctive {
    
    public static PriorityQueue<ResultBlock> conj_res;   // contains results (increasing)
    static int currentDocId;        // target docid
    

    public static PriorityQueue<ResultBlock> executeConjunctive() {

        conj_res = new PriorityQueue<>(k, new ResultBlock.CompareResInc());

        PostingList shortest = ordered_PostingLists.remove(0);

        int counter = 0; 
        do {
            // poll from the shortest posting list
            Posting polled = shortest.getCurrPosting();

            if (polled == null)
                return conj_res;

            currentDocId = polled.getDocId();

            // if the min docid is present for all terms
            if (checkSameDocid()) {

                double score = computeScore(dictionary.getTermStat(shortest.getTerm()).getIdf(), shortest.getCurrPosting());

                for (PostingList pl : ordered_PostingLists)
                    score += computeScore(dictionary.getTermStat(pl.term).getIdf(), pl.getCurrPosting());

                // add results only if the actual score is higher than the lowest score of the results
                if (conj_res.size() < k) {
                    conj_res.add(new ResultBlock(currentDocId, score));
                } else if (conj_res.peek().getScore() < score) {     // sostituisco elemento con peggior score con quello corrente
                    conj_res.remove();
                    conj_res.add(new ResultBlock(currentDocId, score));
                }
                System.out.println(new ResultBlock(currentDocId, score));
               counter++;  
            }
            shortest.next(true);

        } while (shortest.getCurrPosting() != null);

        return conj_res;
    }

    /***
     * Check if all terms have currentDocId in their posting lists.
     * Optionally load the skipping block in which is probable to find it. (maxDocId > currentDocId).
     * @return true if all terms have that docid, false if it encounters the first that does not have it.
     */
    private static boolean checkSameDocid () {

        for (PostingList pl : ordered_PostingLists) {

            // if has skipping but and it is not in the right block (right = maxDocId > currentDocId)
            if((pl.getSl() != null) && (pl.getSl().getCurrSkipElem() != null) && (currentDocId > pl.getSl().getCurrSkipElem().getMaxDocId()))
                pl.nextGEQ(currentDocId, false);
            else        // if no skip or has skip but already in the right block
                while ((pl.getCurrPosting() != null) && (pl.getCurrPosting().getDocId() < currentDocId))
                    pl.next(false);

            if (pl.getCurrPosting() == null || (pl.getCurrPosting() != null && currentDocId != pl.getCurrPosting().getDocId()))
                return false;
        }
        return true;
    }

}
