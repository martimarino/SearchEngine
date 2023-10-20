package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.query.Query.*;

public final class Conjunctive {

    static int currentDocId;
    static ArrayList<PostingList> orderedConjPostingLists;    // posting lists of query terms


    public static void executeConjunctive() {

        // create array of posting lists ordered increasing df
        orderedConjPostingLists = new ArrayList<>();

        for (Map.Entry<Integer, Integer> entry : index_len.entrySet())
            orderedConjPostingLists.add(postingLists.get(entry.getKey()));

        do {
            // poll from the shortest posting list
            Posting polled = orderedConjPostingLists.get(0).getCurrPosting();

            if (polled == null)
                return;

            currentDocId = polled.getDocId();
            if(currentDocId == 13858)
                System.out.println(13858);

            // if the min docid is present for all terms
            if (checkSameDocid()) {

                double score = 0;

                for (PostingList orderedConjPostingList : orderedConjPostingLists)
                    score += Score.computeTFIDF(dictionary.getTermStat(orderedConjPostingList.term).getIdf(), orderedConjPostingList.getCurrPosting());

                if (pq_res.size() < k) {
                    pq_res.add(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score));
                } else if (pq_res.peek().getScore() < score) {     // sostituisco elemento con peggior score con quello corrente
                    pq_res.remove();
                    pq_res.add(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score));
                }
            }
            orderedConjPostingLists.get(0).next(true);

        } while (orderedConjPostingLists.get(0).getCurrPosting() != null);
    }


    private static boolean checkSameDocid () {

        for (PostingList pl : orderedConjPostingLists) {

            while ((pl.getCurrPosting() != null) && (pl.getCurrPosting().getDocId() < currentDocId))
                pl.next(false);

            if (pl.getCurrPosting() != null && currentDocId == pl.getCurrPosting().getDocId())
                continue;

            if (pl.getCurrPosting() == null && pl.getSl() != null)
                pl.nextGEQ(currentDocId);        // search for right block

            if (pl.getCurrPosting() == null)
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
