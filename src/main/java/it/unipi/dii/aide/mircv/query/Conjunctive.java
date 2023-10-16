package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.score.Score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.printError;
import static it.unipi.dii.aide.mircv.utils.Constants.printUI;
import static it.unipi.dii.aide.mircv.Query.*;

public final class Conjunctive {

    static int currentDocId;
    static ArrayList<PostingList> conjPostingLists;    // posting lists of query terms
    static ArrayList<DictionaryElem> arr_de;       // array of DictElem of query terms


    
    public static void executeConjunctive() throws IOException {


        conjPostingLists = new ArrayList<>();    // posting lists of query terms
        arr_de = new ArrayList<>();       // array of DictElem of query terms
        pq_res = new PriorityQueue<>(k, new CompareResInverse());

        // retrieve the (partial or complete) posting list of every query term
        for (String t : query_terms) {

            DictionaryElem de = dictionary.getTermStat(t);
            if (de == null){
               printError("Term " + t + " not present in dictionary!");
               return;
            }

            PostingList tempPL;
            arr_de.add(de);

            if (de.getSkipArrLen() > 0) {   // if there are skipping blocks read partial postings of the first block
                SkipList skipList = new SkipList(de.getSkipOffset(), de.getSkipArrLen());
                SkipInfo skipInfo = skipList.getCurrSkipInfo();

                if(Flags.isCompressionEnabled())
                    tempPL = new PostingList(readCompressedPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen()), skipList);
                else
                    tempPL = new PostingList(readPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getSkipArrLen()), skipList);

            } else {    // read all postings
                if(Flags.isCompressionEnabled())
                    tempPL = new PostingList(readCompressedPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf()), null);
                else
                    tempPL = new PostingList(readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf()), null);
            }

            conjPostingLists.add(tempPL);
        }

        // order posting lists by lenght
        conjPostingLists.sort((pl1, pl2) -> {
            int size1 = pl1.list.size();
            int size2 = pl2.list.size();

            if (size1 < size2) {
                return -1;
            } else if (size1 > size2) {
                return 1;
            } else {
                return 0;
            }
        });

        arr_de.sort((DictionaryElem o1, DictionaryElem o2) -> {
            int df1 = o1.getDf();
            int df2 = o2.getDf();

            if (df1 < df2) {
                return -1;
            } else if (df1 > df2) {
                return 1;
            } else {
                return 0;
            }
        });


        // scorro la lista più corta
        while (conjPostingLists.get(0).currPosting != null) {

            Posting polled = conjPostingLists.get(0).currPosting;
            currentDocId = polled.getDocId();

            // if the min docid is present for all terms
            if(checkSameDocid()) {

                double score = 0;

                for(int j = 0; j < conjPostingLists.size(); j++)
                    score += Score.computeTFIDF(arr_de.get(j).getIdf(), conjPostingLists.get(j).currPosting);

                if(pq_res.isEmpty() || pq_res.size() < k)
                    pq_res.add(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score));
                else if (pq_res.peek().getScore() < score) {     // sostituisco elemento con peggior score con quello corrente
                    if(pq_res.size() == k) {
                        pq_res.poll();
                        pq_res.add(new ResultBlock(documentTable.get(currentDocId).getDocno(), currentDocId, score));
                    }
                }

            }
            conjPostingLists.get(0).next(arr_de.get(0));

        }

        if(pq_res.isEmpty())
            printUI("No results");
        else {
            printUI("\nResults:");
            for (int j = 0; j < k; j++)
                System.out.println(pq_res.poll());
        }

    }

    private static boolean checkSameDocid () {

        ArrayList<Boolean> check = new ArrayList<>();

        for (int j = 0; j < conjPostingLists.size(); j++) {

            PostingList pl = conjPostingLists.get(j);

            if(pl.getCurrPosting().getDocId() == currentDocId)      // already right docid
                check.add(true);
            else {
                if (pl.sl != null)      // skipping present
                    pl.nextGEQ(currentDocId, arr_de.get(j));        // search for tight block

                while (pl.getCurrPosting().getDocId() < currentDocId && pl.currPosting != null)
                    pl.next(arr_de.get(j));

                if(pl.getCurrPosting() == null)
                    check.add(false);
                else
                    check.add(true);
            }
        }
        return !check.contains(false);
    }

}
