package it.unipi.dii.aide.mircv.data_structures;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.*;
import static it.unipi.dii.aide.mircv.data_structures.DictionaryElem.DICT_ELEM_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.delete_tempFiles;

/**
 * class to merge the InverteIndex
 */
public class IndexMerger {
    // Priority queue which will contain the first term (in lexicographic order) of each block. Used for merge and to
    // take from all the blocks the terms in the right order.
    private static PriorityQueue<TermBlock> pq = new PriorityQueue<>(dictionaryBlockOffsets.size() == 0 ? 1 : dictionaryBlockOffsets.size(), new CompareTerm());

    // constructor without parameters
    public IndexMerger() {

    }

    static int i = 0;       // counter used only for control prints
    /**
     *  function to merge the block of the inverted index
     */
    public static void mergeBlocks() {
        System.out.println("Merging partial files...");                     // print of the merging start

        int nrBlocks = DataStructureHandler.dictionaryBlockOffsets.size();  // dictionary number
        DataStructureHandler.readBlockOffsetsFromDisk();        // get offsets of dictionary blocks from disk
        MappedByteBuffer buffer;
        // array containing the current read offset for each block
        ArrayList<Long> currentBlockOffset = new ArrayList<>(nrBlocks);
        currentBlockOffset.addAll(dictionaryBlockOffsets);

        int lim = 200;   // var which indicates the upper limit for control prints, above which no prints will be shown
        int stepProgressionPrint = 100000;  // var which indicates the steps of 'i' progression print during merge

        // open file and create channels for reading the partial dictionary and index file and write the complete index and dictionary file
        try (
                // open partial files to read the partial dictionary and index
                RandomAccessFile partialDocidFile = new RandomAccessFile(PARTIAL_DOCID_FILE, "rw");
                RandomAccessFile partialTermfreqFile = new RandomAccessFile(PARTIAL_TERMFREQ_FILE, "rw");
                RandomAccessFile partialDictFile = new RandomAccessFile(PARTIAL_DICTIONARY_FILE, "rw");
                // open complete files to write the merged dictionary and merged index
                RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile dictFile = new RandomAccessFile(DICTIONARY_FILE, "rw");

                // FileChannel in input (partial file)
                FileChannel dictChannel = partialDictFile.getChannel();
                FileChannel docidChannel = partialDocidFile.getChannel();
                FileChannel termfreqChannel = partialTermfreqFile.getChannel();
                // FileChannel in output (complete file)
                FileChannel outDictionaryChannel = dictFile.getChannel();
                FileChannel outDocIdChannel = docidFile.getChannel();
                FileChannel outTermFreqChannel = termfreqFile.getChannel();
        ) {
            //scroll all blocks and add the first term of each block to priority queue
            for(int i = 0; i <  nrBlocks; i++) {
                buffer = dictChannel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(i), TERM_DIM); //map current block in memory
                String term = StandardCharsets.UTF_8.decode(buffer).toString().split("\0")[0];  // get first term of the block
                pq.add(new TermBlock(term, i));     // add to the priority queue a TermBlock element (term + its blocks number)
            }

            if(verbose){
                System.out.println("\nINITIAL PQ: ");
                for (TermBlock elemento : pq) {
                    System.out.println(elemento.term + " - " + elemento.block);
                }
            }

            // build temp structures
            DictionaryElem tempDE = new DictionaryElem();       // empty temporary DictionaryELem, contains the accumulated data for each term
            ArrayList<Posting> tempPL = new ArrayList<>();      // empty temporary PostingList, contains the accumulated data for each term
            DictionaryElem currentDE;       // current DictionaryElem, contains the data taken from the queue in the current iteration
            ArrayList<Posting> currentPL;   // current PostingList, contains the data taken from the queue in the current iteration

            TermBlock currentTermBlock;     // var that contain the TermBlock extract from pq in the current iteration
            String term = "";   // var that contain the Term of the TermBlock extract from pq in the current iteration
            int block_id = -1;  // var that contain the blockID of the TermBlock extract from pq in the current iteration

            // Merging the posting list.
            // As long as the priority queue is not empty, extract the first term (in lexicographic order) and merge it.
            // After each extraction, the new first term of the block (from which it was previously taken) is taken and
            // put in the priority queue.
            while(!pq.isEmpty()) {
                if (verbose && i < lim)     // print to divide the control print of each iteration
                    System.out.println("-----------------------------------------------------");

                // get first element from priority queue
                currentTermBlock = pq.poll();               // get lowest term from priority queue
                term = currentTermBlock.getTerm();          // get the term
                block_id = currentTermBlock.getBlock();     // get the blockID

                if (verbose && i < lim)
                    System.out.println("Current term (removed from pq): " + currentTermBlock);

                // If there are other elements to be processed in the block identified by block_id (block containing the
                // term taken from the queue in the current iteration) take the next term and add it to the priority queue.
                // If condition divided whether the block considered is the last one or not
                // last block -> check if reading one more element is less than file size
                // not the last block -> check if reading one more element of that block is less than next block start
                // if condition is satisfied -> read new element
                if (currentBlockOffset.get(block_id) + DICT_ELEM_SIZE  < (block_id == (currentBlockOffset.size()-1) ? dictChannel.size() : dictionaryBlockOffsets.get(block_id +1))){
                    buffer = dictChannel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(block_id) + DICT_ELEM_SIZE, TERM_DIM); // get first element of the block
                    String[] t = StandardCharsets.UTF_8.decode(buffer).toString().split("\0");      // get the term of element
                    if (!(t.length == 0))           // control check if term is not empty
                        pq.add(new TermBlock(t[0], block_id));  //add to the priority queue a term block element (term + its blocks number)
                    if (verbose && i < lim)
                        System.out.println("New term (added to pq) -> TERM: " + Arrays.toString(t) + " - FROM BLOCK: " + block_id);
                }
                else
                {
                    System.out.println("current block offset " + currentBlockOffset.get(block_id) + "block "+ block_id);
                }

                // print the current element in the priority queue
                if (verbose && i < lim) {
                    System.out.println("ACTUAL PQ: ");
                    for (TermBlock elemento : pq) {
                        System.out.println(elemento.term + " - " + elemento.block);
                    }
                }

                // get current elem of dictionary
                currentDE = readDictionaryElemFromDisk(currentBlockOffset.get(block_id), dictChannel);
                // get current posting list
                currentPL = readPostingListFromDisk(currentDE.getOffsetDocId(), currentDE.getOffsetTermFreq(), currentDE.getDf(), docidChannel, termfreqChannel);

                if(verbose && i < lim) {
                    System.out.println("CURR DE: " + currentDE);
                    System.out.println("CURR PL: " + currentPL.size());
                }

                if (tempDE.getTerm().equals("")) {        // first iteration

                    //set temp variables values with value of the element taken in the current iteration
                    tempDE = currentDE;
                    tempDE.setOffsetTermFreq(outTermFreqChannel.size());
                    tempDE.setOffsetDocId(outDocIdChannel.size());
                    tempPL = currentPL;

                    if(verbose && i < lim) {
                        System.out.println("*** First iteration ***");
                        System.out.println("TEMP DE: " + tempDE);
                        System.out.println("TEMP PL: " + tempPL.size());
                    }
                } else {                                // is not the first iteration
                    if(verbose && i < lim) {
                        System.out.println("TEMP DE: " + tempDE);
                        System.out.println("TEMP PL: " + tempPL.size());
                    }

                    // same term found (respect the previous iteration), temporary structures update
                    if (currentDE.getTerm().equals(tempDE.getTerm())) {
                        if(verbose && i < lim)
                            System.out.println("*** Same term of previous one -> accumulate on temp variables ***");

                        // update DictionaryElem
                        tempDE.addCf(currentDE.getCf());        // update Cf
                        tempDE.addDf(currentDE.getDf());        // update Df

                        assert tempPL != null;
                        tempPL.addAll(currentPL);               // add all new postings

                        if(verbose && i < lim) {
                            System.out.println("TEMP DE: " + tempDE);
                            System.out.println("TEMP PL: " + tempPL.size());
                        }
                    }
                    else{    // different term found (respect the previous iteration), write to disk the complete data of the previous term
                        if(verbose && i < lim) {
                            System.out.println("*** Writing elem to disk... ***");
                            System.out.println("Temp variables status (with the elem to be written)");
                            System.out.println("TEMP DE: " + tempDE);
                            System.out.println("TEMP PL: " + tempPL.size());
                        }

                        // write DictionaryElem to disk
                        storeDictionaryElemIntoDisk(tempDE, outDictionaryChannel);
                        // write InvertedIndexElem to disk
                        storePostingListIntoDisk(tempPL, outTermFreqChannel, outDocIdChannel);

                        //set temp variables values
                        tempDE = currentDE;
                        tempPL = currentPL;

                        if(verbose && i < lim) {
                            System.out.println("Temp variables status after writing and update");
                            System.out.println("TEMP DE: " + tempDE);
                            System.out.println("TEMP PL: " + tempPL.size());
                        }
                    }
                }
                // update the offset of next element to read from the block read in this iteration
                currentBlockOffset.set(block_id, currentBlockOffset.get(block_id) + DICT_ELEM_SIZE);

                if (verbose && (i % stepProgressionPrint == 0))      // print to visualize the progression of the merge
                    System.out.println("i: " + i);
                i++;                                    // increment the counter
            }
            if (verbose)      // print to visualize the total number of iterations
                System.out.println("Merge ended, total number of iterations (i) is: " + i);
//            delete_tempFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * class to define TermBlock. The priority queue contains instances of TermBlock
     */
    private static class TermBlock {
        String term;    // string of the term related to TermBlock
        int block;      // reference to the id of the block in which are the data

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
