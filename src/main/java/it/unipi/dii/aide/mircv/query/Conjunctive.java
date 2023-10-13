package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.Query;
import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.printError;

public final class Conjunctive {

    static int currentDocId;
    static int k;

    static PriorityQueue<DAATBlock> pq = new PriorityQueue<>();
    static PriorityQueue<ResultBlock> results = new PriorityQueue<>(k, new CompareRes());

    static ArrayList<PostingList> conjPostingLists = new ArrayList<>();    // posting lists of query terms



    public static void executeConjunctive(ArrayList<String> query, int k, FileChannel docIdChannel, FileChannel termFreqChannel, FileChannel skipChannel) throws IOException {

        HashMap<String, DictionaryElem> arr_de = new HashMap<>();       // array of DictElem of query terms
        Conjunctive.k = k;

        // retrieve the (partial or complete) posting list of every query term
        for (String t : query) {
            DictionaryElem de = Query.dictionary.getTermStat(t);
            if (de == null){
               printError("Term " + t + " not present in dictionary!");
               return;
            }

            arr_de.put(t, de);
            PostingList tempPL = new PostingList();

            if (de.getSkipArrLen() > 0) {   // if there are skipping blocks read partial postings of the first block
                SkipList skipList = new SkipList(de.getSkipOffset(), de.getSkipArrLen(), skipChannel);
                SkipInfo skipInfo = skipList.getCurrSkipInfo();

                if(Flags.isCompressionEnabled())
                    tempPL.list = readCompressedPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen(), docIdChannel, termFreqChannel);
                else
                    tempPL.list = readPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getSkipArrLen(), docIdChannel, termFreqChannel);

                tempPL.sl = skipList;

            } else {    // read all postings
                if(Flags.isCompressionEnabled())
                    tempPL.list = readCompressedPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf(), docIdChannel, termFreqChannel);
                else
                    tempPL.list = readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf(),docIdChannel,termFreqChannel);

                conjPostingLists.add(tempPL);
            }
            pq.add(new DAATBlock(t, tempPL.list.get(0).getDocId(), de.getIdf()));
        }

        while (!pq.isEmpty()) {

            DAATBlock polled = pq.poll();
            currentDocId = polled.getDocId();
            DictionaryElem currDE = arr_de.get(polled.getTerm());

            // if the min docid is present for all terms
            if(checkDocidInAllPostingLists(currDE, docIdChannel, termFreqChannel)) {
                if(results.peek().getScore() < currDE.getIdf())
                    results.add(new ResultBlock(Query.documentTable.get(currentDocId).getDocno(), currentDocId, currDE.computeIdf()));
            }

        }

    }

    private static boolean checkDocidInAllPostingLists(DictionaryElem de, FileChannel docIdChannel, FileChannel termFreqChannel) {

        for (PostingList pl : conjPostingLists) {
            pl.nextGEQ(currentDocId, de, docIdChannel, termFreqChannel);

            if(pl.getCurrPosting() == null)
                return false;
        }

        return true;
    }


}
