package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.query.scores.Score;

import java.util.*;

import static it.unipi.dii.aide.mircv.query.Query.*;

public class Conjunctive {

    static ArrayList<PostingList> orderedConjPostingLists;
    static int currentDocId;

    public static void executeConjunctive() {

        // create array of posting lists ordered increasing df
        orderedConjPostingLists = new ArrayList<>();
        PostingList shortest;

        index_len = (HashMap<Integer, Integer>) sortByValue(index_len);

        for (Map.Entry<Integer, Integer> entry : index_len.entrySet())
            orderedConjPostingLists.add(postingLists.get(entry.getKey()));

        shortest = orderedConjPostingLists.remove(0);

        postingLists.clear();
        index_len.clear();

        do {
            // poll from the shortest posting list
            Posting polled = shortest.getCurrPosting();

            if (polled == null)
                return;

            currentDocId = polled.getDocId();

            // if the min docid is present for all terms
            if (checkSameDocid()) {

                double score = Score.computeTFIDF(dictionary.getTermStat(shortest.getTerm()).getIdf(), shortest.getCurrPosting());

                for (PostingList orderedConjPostingList : orderedConjPostingLists)
                    score += Score.computeTFIDF(dictionary.getTermStat(orderedConjPostingList.term).getIdf(), orderedConjPostingList.getCurrPosting());


                if (pq_res.size() < k) {
                    pq_res.add(new ResultBlock(currentDocId, score));
                } else if (pq_res.peek().getScore() < score) {     // sostituisco elemento con peggior score con quello corrente
                    pq_res.remove();
                    pq_res.add(new ResultBlock(currentDocId, score));
                }
                System.out.println(new ResultBlock(currentDocId, score));
            }
            shortest.next(true);

        } while (shortest.getCurrPosting() != null);

        orderedConjPostingLists.clear();

    }

    private static boolean checkSameDocid () {

        for (PostingList pl : orderedConjPostingLists) {

            if((pl.getSl() != null) && (pl.getSl().getCurrSkipInfo() != null) && (currentDocId > pl.getSl().getCurrSkipInfo().getMaxDocId()))
                pl.nextGEQ(currentDocId);
            else
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
