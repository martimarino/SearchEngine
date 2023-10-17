package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.score.Score;

import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.printDebug;
import static it.unipi.dii.aide.mircv.utils.Constants.printError;
import static it.unipi.dii.aide.mircv.Query.*;

public final class Conjunctive {

    static int currentDocId;
    static ArrayList<PostingList> conjPostingLists;    // posting lists of query terms
    static ArrayList<PostingList> orderedConjPostingLists;    // posting lists of query terms
    static HashMap<Integer, Integer> index_len;

    
    public static void executeConjunctive() throws IOException {

        conjPostingLists = new ArrayList<>();    // posting lists of query terms
        index_len = new HashMap<>();

        // retrieve the (partial or complete) posting list of every query term
        for (String t : query_terms) {

            DictionaryElem de = dictionary.getTermStat(t);
            if (de == null) {
                printError("Term " + t + " not present in dictionary!");
                return;
            }

            index_len.put(query_terms.indexOf(t), de.getDf());
            PostingList tempPL = null;

            if (de.getSkipArrLen() > 0) {   // if there are skipping blocks read partial postings of the first block
                SkipList skipList = new SkipList(de.getSkipOffset(), de.getSkipArrLen());
                if(tempPL.term.equals("of"))
                    for(SkipInfo si : skipList.getArr_skipInfo())
                        System.out.println("SKIP INFO READ (term for): " + si);
                SkipInfo skipInfo = skipList.getCurrSkipInfo();

                if (Flags.isCompressionEnabled())
                    tempPL = new PostingList(t, readCompressedPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen()), skipList, de.getTermFreqSize(), de.getDocIdSize());
                else
                    tempPL = new PostingList(t, readPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getSkipArrLen()), skipList, de.getTermFreqSize(), de.getDocIdSize());

            } else {    // read all postings
                if (Flags.isCompressionEnabled())
                    tempPL = new PostingList(t, readCompressedPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf()), null, de.getTermFreqSize(), de.getDocIdSize());
                else
                    tempPL = new PostingList(t, readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf()), null, de.getTermFreqSize(), de.getDocIdSize());
            }

            conjPostingLists.add(tempPL);
        }

        // order posting lists indices by increasing order
        index_len = (HashMap<Integer, Integer>) sortByValue(index_len);

        orderedConjPostingLists = new ArrayList<>();

        for (Map.Entry<Integer, Integer> entry : index_len.entrySet())
            orderedConjPostingLists.add(conjPostingLists.get(entry.getKey()));


        // scorro la lista più corta
        while (orderedConjPostingLists.get(0).currPosting != null) {

            Posting polled = orderedConjPostingLists.get(0).currPosting;
            currentDocId = polled.getDocId();

            // if the min docid is present for all terms
            if (checkSameDocid()) {

                double score = 0;

                for (PostingList orderedConjPostingList : orderedConjPostingLists)
                    score += Score.computeTFIDF(dictionary.getTermStat(orderedConjPostingList.term).getIdf(), orderedConjPostingList.currPosting);

                if (inverseResultQueue.isEmpty() || inverseResultQueue.size() < k)
                    inverseResultQueue.add(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score));
                else if (inverseResultQueue.peek().getScore() < score) {     // sostituisco elemento con peggior score con quello corrente
                    if (inverseResultQueue.size() == k) {
                        printDebug(String.valueOf(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score)));
                        inverseResultQueue.poll();
                        inverseResultQueue.add(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score));
                    }
                }
            }
            orderedConjPostingLists.get(0).next();
        }
    }


    private static boolean checkSameDocid () {

        for (PostingList pl : orderedConjPostingLists) {

            if (pl.currPosting != null && currentDocId == pl.getCurrPosting().getDocId()) {
                continue;
            }     // already right docid
            if (pl.sl != null)      // skipping present
                pl.nextGEQ(currentDocId);        // search for tight block

            while (pl.getCurrPosting() != null && pl.getCurrPosting().getDocId() < currentDocId)
                pl.next();

            if (pl.getCurrPosting() == null || pl.getCurrPosting().getDocId() != currentDocId)
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
