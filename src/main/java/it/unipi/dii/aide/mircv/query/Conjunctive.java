package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.Query;
import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.printError;

public final class Conjunctive {

    static int currentDocId;

    static PriorityQueue<ResultBlock> results;
    static ArrayList<PostingList> conjPostingLists = new ArrayList<>();    // posting lists of query terms



    public static void executeConjunctive() throws IOException {

        ArrayList<DictionaryElem> arr_de = new ArrayList<>();       // array of DictElem of query terms
        results = new PriorityQueue<>(Query.k, new CompareRes());

        // retrieve the (partial or complete) posting list of every query term
        for (String t : Query.query) {

            DictionaryElem de = Query.dictionary.getTermStat(t);
            if (de == null){
               printError("Term " + t + " not present in dictionary!");
               return;
            }

            arr_de.add(de);
            PostingList tempPL = new PostingList();

            if (de.getSkipArrLen() > 0) {   // if there are skipping blocks read partial postings of the first block
                SkipList skipList = new SkipList(de.getSkipOffset(), de.getSkipArrLen());
                SkipInfo skipInfo = skipList.getCurrSkipInfo();

                if(Flags.isCompressionEnabled())
                    tempPL.list = readCompressedPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen());
                else
                    tempPL.list = readPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getSkipArrLen());

                tempPL.sl = skipList;

            } else {    // read all postings
                if(Flags.isCompressionEnabled())
                    tempPL.list = readCompressedPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf());
                else
                    tempPL.list = readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf());
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

        // scorro la lista più corta
        while (conjPostingLists.get(0).currPosting != null) {

            Posting polled = conjPostingLists.get(0).currPosting;
            currentDocId = polled.getDocId();
            DictionaryElem currDE = arr_de.get(0);

            // if the min docid is present for all terms
            if(checkSameDocid(currDE)) {
                if(results.peek().getScore() < currDE.getIdf()) {       // sostituisco elemento con peggior score con quello corrente
                    results.poll();
                    results.add(new ResultBlock(Query.documentTable.get(currentDocId).getDocno(), currentDocId, currDE.getIdf()));
                }
            }
            conjPostingLists.get(0).next(arr_de.get(0));

        }

        for (int i = 0; i < Query.k && !results.isEmpty(); i++) {
            System.out.println(results.poll());
        }

    }

    private static boolean checkSameDocid(DictionaryElem de) {

        for (PostingList pl : conjPostingLists) {
            pl.nextGEQ(currentDocId, de);

            if(pl.getCurrPosting() == null)
                return false;
        }

        return true;
    }

}
