package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.query.scores.Score;

import java.util.*;

import static it.unipi.dii.aide.mircv.query.Query.*;
import static it.unipi.dii.aide.mircv.utils.Constants.printDebug;

public class Conjunctive {
    
    public static PriorityQueue<ResultBlock> conj_res;   // contains results (increasing)
    static ArrayList<PostingList> orderedConjPostingLists;      // posting lists ordered by lenght
    static int currentDocId;        // target docid
    

    public static PriorityQueue<ResultBlock> executeConjunctive() {

        conj_res = new PriorityQueue<>(k, new ResultBlock.CompareRes());

        // create array of posting lists ordered increasing df
        orderedConjPostingLists = new ArrayList<>();
        PostingList shortest;

        // sort indices
        index_len = (HashMap<Integer, Integer>) sortByValue(index_len);

        // copy list using ordered indices
        for (Map.Entry<Integer, Integer> entry : index_len.entrySet())
            orderedConjPostingLists.add(postingLists.get(entry.getKey()));

        shortest = orderedConjPostingLists.remove(0);

        postingLists.clear();
        index_len.clear();

        int counter = 0; 
        do {
            // poll from the shortest posting list
            Posting polled = shortest.getCurrPosting();

            if (polled == null)
                return conj_res;

            currentDocId = polled.getDocId();

            // if the min docid is present for all terms
            if (checkSameDocid()) {

                double score = Score.computeTFIDF(dictionary.getTermStat(shortest.getTerm()).getIdf(), shortest.getCurrPosting());

                for (PostingList orderedConjPostingList : orderedConjPostingLists)
                    score += Score.computeTFIDF(dictionary.getTermStat(orderedConjPostingList.term).getIdf(), orderedConjPostingList.getCurrPosting());

                // add results only if the actual score is higher than the lowest score of the results
                if (conj_res.size() < k) {
                    conj_res.add(new ResultBlock(currentDocId, score));
                } else if (conj_res.peek().getScore() < score) {     // sostituisco elemento con peggior score con quello corrente
                    conj_res.remove();
                    conj_res.add(new ResultBlock(currentDocId, score));
                }
               counter++;  
            }
            shortest.next(true);

        } while (shortest.getCurrPosting() != null);
        
        printDebug("FIND: " + counter);
        return conj_res;
    }

    /***
     * Check if all terms have currentDocId in their posting lists.
     * Optionally load the skipping block in which is probable to find it. (maxDocId > currentDocId).
     * @return true if all terms have that docid, false if it encounters the first that does not have it.
     */
    private static boolean checkSameDocid () {

        for (PostingList pl : orderedConjPostingLists) {

            // if has skipping but and it is not in the right block (right = maxDocId > currentDocId)
            if((pl.getSl() != null) && (pl.getSl().getCurrSkipInfo() != null) && (currentDocId > pl.getSl().getCurrSkipInfo().getMaxDocId()))
                pl.nextGEQ(currentDocId, false);
            else        // if no skip or has skip but already in the right block
                while ((pl.getCurrPosting() != null) && (pl.getCurrPosting().getDocId() < currentDocId))
                    pl.next(false);

            if (pl.getCurrPosting() == null || (pl.getCurrPosting() != null && currentDocId != pl.getCurrPosting().getDocId()))
                return false;
        }
        return true;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {

        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Map<K, V> result = new LinkedHashMap<>();

        for (Map.Entry<K, V> entry : list)
            result.put(entry.getKey(), entry.getValue());

        return result;
    }
}
