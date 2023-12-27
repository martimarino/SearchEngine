package it.unipi.dii.aide.mircv.index;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.*;
import static it.unipi.dii.aide.mircv.data_structures.DictionaryElem.getDictElemSize;
import static it.unipi.dii.aide.mircv.query.scores.Score.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static java.lang.Math.min;

/**
 * class to merge the InvertedIndex
 */
public final class IndexMerger {

    // Priority queue which will contain the first term (in lexicographic order) of each block. Used for merge and to
    // take from all the blocks the terms in the right order.
    private static PriorityQueue<TermBlock> pq = new PriorityQueue<>(dictionaryBlockOffsets.isEmpty() ? 1 : dictionaryBlockOffsets.size(), new CompareTerm());

    private IndexMerger() {
        throw new UnsupportedOperationException();
    }

    static int termCounter = 0;       // counter used only for control prints

    // build temp structures
    static DictionaryElem tempDE;       // temporary DictionaryELem, contains the accumulated data for each term
    static ArrayList<Posting> tempPL;      // temporary PostingList, contains the accumulated data for each term
    static DictionaryElem currentDE;       // current DictionaryElem, contains the data taken from the queue in the current iteration
    static ArrayList<Posting> currentPL;   // current PostingList, contains the data taken from the queue in the current iteration

    /**
     *  function to merge the block of the inverted index
     */
    public static void mergeBlocks() {

        // open file and create channels for reading the partial dictionary and index file and write the complete index and dictionary file
        try (
                // open complete files to write the merged dictionary and merged index
                RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile dictFile = new RandomAccessFile(DICTIONARY_FILE, "rw");
                // open skipping file
                RandomAccessFile skipFile = new RandomAccessFile(SKIP_FILE, "rw")
        ){

            // FileChannel in output (complete file)
            dict_channel = dictFile.getChannel();
            docId_channel = docidFile.getChannel();
            termFreq_channel = termfreqFile.getChannel();
            // FileChannel in output (skipping)
            skip_channel = skipFile.getChannel();

            System.out.println("\nMerging partial files...");                     // print of the merging start

            //DataStructureHandler.readBlockOffsetsFromDisk();        // get offsets of dictionary blocks from disk
            int nrBlocks = dictionaryBlockOffsets.size();           // dictionary number
            MappedByteBuffer buffer;
            // array containing the current read offset for each block
            ArrayList<Long> currentBlockOffset = new ArrayList<>(nrBlocks);
            currentBlockOffset.addAll(dictionaryBlockOffsets);
            readDocumentTableFromDisk();
            // var which indicates the steps of 'i' progression print during merge
            System.out.println("Compression " + Flags.isCompressionEnabled());

            // scroll all blocks and add the first term of each block to priority queue
            for (int i = 0; i < nrBlocks; i++) {
                buffer = partialDict_channel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(i), TERM_DIM); //map current block in memory
                String term = StandardCharsets.UTF_8.decode(buffer).toString().split("\0")[0];  // get first term of the block
                pq.add(new TermBlock(term, i));     // add to the priority queue a TermBlock element (term + its blocks number)
            }

            // build temp structures
            tempDE = new DictionaryElem();       // empty temporary DictionaryELem, contains the accumulated data for each term
            tempPL = new ArrayList<>();      // empty temporary PostingList, contains the accumulated data for each term
            currentDE = new DictionaryElem();       // current DictionaryElem, contains the data taken from the queue in the current iteration
            currentPL = new ArrayList<>();   // current PostingList, contains the data taken from the queue in the current iteration

            TermBlock currentTermBlock;     // var that contain the TermBlock extract from pq in the current iteration
            String term = "";   // var that contain the Term of the TermBlock extract from pq in the current iteration
            int block_id = -1;  // var that contain the blockID of the TermBlock extract from pq in the current iteration

            while(!pq.isEmpty()) {

                // get first element from priority queue
                currentTermBlock = pq.poll();               // get lowest term from priority queue
                term = currentTermBlock.getTerm();          // get the term
                block_id = currentTermBlock.getBlock();     // get the blockID

                if (currentBlockOffset.get(block_id) + getDictElemSize() < (block_id == (currentBlockOffset.size() - 1) ? partialDict_channel.size() : dictionaryBlockOffsets.get(block_id + 1))) {
                    buffer = partialDict_channel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(block_id) + getDictElemSize(), TERM_DIM); // get first element of the block
                    String[] t = StandardCharsets.UTF_8.decode(buffer).toString().split("\0");      // get the term of element
                    if (!(t.length == 0))           // control check if term is not empty
                        pq.add(new TermBlock(t[0], block_id));  //add to the priority queue a term block element (term + its blocks number)
                }

                // get current elem of dictionary
                currentDE.readDictionaryElemFromDisk(currentBlockOffset.get(block_id));
                // get current posting list
                currentPL = readPostingListFromDisk(currentDE.getOffsetDocId(), currentDE.getOffsetTermFreq(), currentDE.getDf(), partialDocId_channel, partialTermFreq_channel);

                if (tempDE.getTerm().isEmpty()) {        // first iteration

                    //set temp variables values with value of the element taken in the current iteration
                    tempDE = currentDE;
                    tempDE.setOffsetTermFreq(termFreq_channel.size());
                    tempDE.setOffsetDocId(docId_channel.size());
                    tempPL = currentPL;

                } else {                                // is not the first iteration

                    // same term found (respect the previous iteration), temporary structures update
                    if (currentDE.getTerm().equals(tempDE.getTerm())) {

                        // update DictionaryElem
                        tempDE.addCf(currentDE.getCf());        // update Cf
                        tempDE.addDf(currentDE.getDf());        // update Df

                        assert tempPL != null;
                        tempPL.addAll(currentPL);               // add all new postings
                    }
                    else{    // different term found, write to disk the complete data of the previous term

                        writeOnDisk();

                        //set temp variables values
                        tempDE = currentDE;
                        tempPL = currentPL;
                    }
                }
                currentDE = new DictionaryElem();

                // update the offset of next element to read from the block read in this iteration
                currentBlockOffset.set(block_id, currentBlockOffset.get(block_id) + getDictElemSize());
            }

            writeOnDisk();

            printDebug("Num terms: " + termCounter);
            printDebug("Merge ended, total number of iterations (i) is: " + termCounter);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeOnDisk() throws IOException {

        Flags.setConsiderSkipElem(true);
        tempDE.setIdf(tempDE.computeIdf());

        if (Flags.isDebug_flag()) {
            saveIntoFile("TERM: '" + tempDE.getTerm() + "'", "merge_pl.txt");
            saveIntoFile("TERM: '" + tempDE.getTerm() + "'", "merge_docid.txt");
        }
        //update DocID and Term Frequency offset ( equal to the end of the files)
        tempDE.setOffsetTermFreq(termFreq_channel.size());
        tempDE.setOffsetDocId(docId_channel.size());

        assert tempPL != null;
        int lenPL = tempPL.size();

        int[] tempCompressedLength = new int[2];

        if(lenPL >= SKIP_POINTERS_THRESHOLD) {
            int skipInterval = (int) Math.ceil(Math.sqrt(lenPL));        // one skip every sqrt(docs)
            int nSkip = 0;

            tempDE.setSkipListOffset(skip_channel.size());

            for(int i = 0; i < lenPL; i += skipInterval) {
                List<Posting> subPL = tempPL.subList(i, min(i + skipInterval, lenPL));
                ArrayList<Posting> tempSubPL = new ArrayList<>(subPL);
                if(tempDE.getTerm().equals("berlin") || tempDE.getTerm().equals("center"))
                    for(Posting p : subPL)
                        saveIntoFile(" docid: " + p.getDocId() + " tf: " + p.getTermFreq(), tempDE.getTerm() + ".txt");
                if (Flags.isCompressionEnabled()) {
                    SkipElem sp = new SkipElem(tempSubPL.get(tempSubPL.size()-1).getDocId(), docId_channel.size(), termFreq_channel.size(), -1, -1);
                    tempDE.setMaxBM25(computeMaxBM25(tempSubPL, tempDE.getIdf()));
                    tempDE.setMaxTFIDF(computeMaxTFIDF(tempSubPL, tempDE.getIdf()));
                    int[] compressedLength = DataStructureHandler.storeCompressedPostingIntoDisk(tempSubPL);//store index with compression - unary compression for termfreq
                    assert compressedLength != null;
                    tempCompressedLength[0] += compressedLength[0];// tf
                    tempCompressedLength[1] += compressedLength[1]; //docid
                    sp.setTermFreqBlockLen(compressedLength[0]);
                    sp.setDocIdBlockLen(compressedLength[1]);
                    sp.storeSkipElemToDisk();
                } else {
                    SkipElem sp = new SkipElem(tempSubPL.get(tempSubPL.size()-1).getDocId(), docId_channel.size(), termFreq_channel.size(), tempSubPL.size(), tempSubPL.size());
                    sp.storeSkipElemToDisk();
                    double[] score = storePostingListIntoDisk(tempSubPL, tempDE.getIdf());
                    assert score != null;
                    tempDE.setMaxBM25(score[0]);
                    tempDE.setMaxTFIDF(score[1]);
                }
                nSkip++;
            }
            tempDE.setSkipListLen(nSkip);

            if(Flags.isCompressionEnabled()) {
                tempDE.setTermFreqSize(tempCompressedLength[0]);
                tempDE.setDocIdSize(tempCompressedLength[1]);
            }
        }
        else {
            if(Flags.isCompressionEnabled()){
                tempDE.setMaxBM25(computeMaxBM25(tempPL, tempDE.getIdf()));
                tempDE.setMaxTFIDF(computeMaxTFIDF(tempPL, tempDE.getIdf()));
                int[] compressedLength = DataStructureHandler.storeCompressedPostingIntoDisk(tempPL);//store index with compression - unary compression for termfreq
                assert compressedLength != null;
                tempDE.setTermFreqSize(compressedLength[0]);
                tempDE.setDocIdSize(compressedLength[1]);
            }
            else {
                double[] score = storePostingListIntoDisk(tempPL, tempDE.getIdf());
                tempDE.setMaxBM25(score[0]);
                tempDE.setMaxTFIDF(score[1]);
            }
        }
        tempDE.storeDictionaryElemIntoDisk(dict_channel);
        termCounter++;
        Flags.setConsiderSkipElem(false);

    }

    /**
     * class to define TermBlock. The priority queue contains instances of TermBlock
     */
    private static class TermBlock {
        final String term;    // string of the term related to TermBlock
        final int block;      // reference to the id of the block in which are the data

        // constructor with parameters
        public TermBlock(String term, int block) {
            this.term = term;
            this.block = block;
        }

        public String getTerm() {
            return term;
        }

        public int getBlock() {
            return block;
        }

        @Override
        public String toString() {
            return "TermBlock{" +
                    "term='" + term + '\'' +
                    ", block=" + block +
                    '}';
        }
    }

    /**
     * class to compare the block, allows the order of the priority queue
     */
    private static class CompareTerm implements Comparator<TermBlock> {
        @Override
        public int compare(TermBlock tb1, TermBlock tb2) {
            // comparing terms
            int termComparison = tb1.getTerm().compareTo(tb2.getTerm());
            // if the terms are equal, compare by block number
            if (termComparison == 0) {
                return Integer.compare(tb1.getBlock(), tb2.getBlock());
            }

            return termComparison;
        }
    }
}