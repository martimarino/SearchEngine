package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.Query;
import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.score.Score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.printError;
import static it.unipi.dii.aide.mircv.utils.Constants.printUI;

public final class Conjunctive {

    static int currentDocId;

    static PriorityQueue<ResultBlock> results;
    static ArrayList<PostingList> conjPostingLists = new ArrayList<>();    // posting lists of query terms
    static ArrayList<DictionaryElem> arr_de = new ArrayList<>();       // array of DictElem of query terms

    
    public static void executeConjunctive() throws IOException {


        results = new PriorityQueue<>(Query.k, new CompareResInverse());
        int i = 0;

        // retrieve the (partial or complete) posting list of every query term
        for (String t : Query.query_terms) {

            DictionaryElem de = Query.dictionary.getTermStat(t);
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
            i++;
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


        DictionaryElem firstDE = arr_de.get(0); // ordinare de crescenti

        // scorro la lista più corta
        while (conjPostingLists.get(0).postingIterator.hasNext()) {

            Posting polled = conjPostingLists.get(0).currPosting;
            currentDocId = polled.getDocId();

            // if the min docid is present for all terms
            if(checkSameDocid()) {

                double score = 0;

                for(int j = 0; j < conjPostingLists.size(); j++)
                    score += Score.computeTFIDF(arr_de.get(j).getIdf(), conjPostingLists.get(j).currPosting);

                System.out.println("Score: " + score);

                if(results.isEmpty() || results.size() < Query.k)
                    results.add(new ResultBlock(Query.documentTable.get(currentDocId).getDocno(), currentDocId, score));
                else if (results.peek().getScore() < firstDE.getIdf()) {     // sostituisco elemento con peggior score con quello corrente
                    if(results.size() == Query.k) {
                        results.poll();
                        results.add(new ResultBlock(Query.documentTable.get(currentDocId).getDocno(), currentDocId, score));
                    }
                }

            }
            conjPostingLists.get(0).next(arr_de.get(0));

        }

        printUI("\nResults:");
        for (int j = 0; j < Query.k && !results.isEmpty(); j++) {
            System.out.println(results.poll());
        }

    }

    private static boolean checkSameDocid () {

        System.out.println("Check for id: " + currentDocId);

        ArrayList<Boolean> check = new ArrayList<>();

        for (int j = 0; j < conjPostingLists.size(); j++) {

            PostingList pl = conjPostingLists.get(j);

            if(pl.getCurrPosting().getDocId() == currentDocId)
                check.add(true);
            else {
                if (pl.sl != null)
                    pl.nextGEQ(currentDocId, arr_de.get(j));
                else
                    while (pl.getCurrPosting().getDocId() < currentDocId)
                        pl.next(arr_de.get(j));
            }

//            if(pl.getCurrPosting() == null || !(pl.getCurrPosting().getDocId() == currentDocId))
            if(pl.getCurrPosting() == null)
                return false;
        }
        System.out.println((!check.contains(false) ? "check ok" : "check not ok"));
        return !check.contains(false);
    }

}
