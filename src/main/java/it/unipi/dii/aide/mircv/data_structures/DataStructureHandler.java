package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.TextProcessor;
import it.unipi.dii.aide.mircv.compression.Unary;
import it.unipi.dii.aide.mircv.QueryProcessor;
import it.unipi.dii.aide.mircv.compression.VariableBytes;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.data_structures.DocumentElement.*;
import static it.unipi.dii.aide.mircv.data_structures.PartialIndexBuilder.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

/**
 * This class handles the storage and retrieval of data structures used for document indexing.
 */
public final class DataStructureHandler {

    // -------- start: functions to store into disk --------

    // function to store the whole document table into disk
    static void storeDocumentTableIntoDisk() {

        try (RandomAccessFile raf = new RandomAccessFile(DOCTABLE_FILE, "rw");
             FileChannel channel = raf.getChannel()) {

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), (long) DOCELEM_SIZE * PartialIndexBuilder.documentTable.size());

            // Buffer not created
            if(buffer == null)
                return;
            // scan all document elements of the Document Table
            for(DocumentElement de: PartialIndexBuilder.documentTable.values()) {
                //allocate bytes for docno
                CharBuffer charBuffer = CharBuffer.allocate(DOCNO_DIM);

                //put every char into charbuffer
                for (int i = 0; i < de.getDocno().length(); i++)
                    charBuffer.put(i, de.getDocno().charAt(i));

                // write docno, docid and doclength into document file
                buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
                buffer.putInt(de.getDocid());
                buffer.putInt(de.getDoclength());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // function to store offset of the blocks into disk
    static void storeBlockOffsetsIntoDisk() {
        System.out.println("\nStoring block offsets into disk...");

        try (
                RandomAccessFile raf = new RandomAccessFile(BLOCKOFFSETS_FILE, "rw");
                FileChannel channel = raf.getChannel()
        ) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) LONG_BYTES * dictionaryBlockOffsets.size()); //offset_size (size of dictionary offset) * number of blocks

            // Buffer not created
            if(buffer == null)
                return;

            // scan all block and for each one write offset into disk
            for (int i = 0; i < dictionaryBlockOffsets.size(); i++) {
                printDebug("OFFSET BLOCK " + i + ": " + dictionaryBlockOffsets.get(i));
                buffer.putLong(dictionaryBlockOffsets.get(i)); //store into file the dictionary offset of the i-th block
            }

            System.out.println(dictionaryBlockOffsets.size() + " blocks stored");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // function to store Dictionary and Inverted Index into disk
    public static void storeIndexAndDictionaryIntoDisk() {

        try (
                RandomAccessFile docidFile = new RandomAccessFile(PARTIAL_DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(PARTIAL_TERMFREQ_FILE, "rw");
                RandomAccessFile dictFile = new RandomAccessFile(PARTIAL_DICTIONARY_FILE, "rw");
                FileChannel docidChannel = docidFile.getChannel();
                FileChannel termfreqChannel = termfreqFile.getChannel();
                FileChannel dictChannel = dictFile.getChannel()
        ) {
            dictionary.sort();              // Sort the dictionary lexicographically
            dictionaryBlockOffsets.add(PARTIAL_DICTIONARY_OFFSET);// update of the offset of the block for the dictionary file

            // iterate through all the terms of the dictionary ordered
            for (String term : dictionary.getTermToTermStat().keySet()) {

                //get posting list of the term
                ArrayList<Posting> posList = invertedIndex.get(term);

                //create dictionary element for the term
                DictionaryElem dictElem = dictionary.getTermStat(term);
                dictElem.setOffsetTermFreq(INDEX_OFFSET);
                dictElem.setOffsetDocId(INDEX_OFFSET);
                if(term.equals("0000"))
                    printDebug("term: 0000 " + dictionary.getTermToTermStat().get("0000") +  " block " + (dictionaryBlockOffsets.size()-1) + " size: " + posList.size());

                // Create buffers for docid and termfreq
                MappedByteBuffer buffer_docid = docidChannel.map(FileChannel.MapMode.READ_WRITE, docidChannel.size(), (long) posList.size() * INT_BYTES); // from 0 to number of postings * int dimension
                MappedByteBuffer buffer_termfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, termfreqChannel.size(), (long) posList.size() * INT_BYTES); //from 0 to number of postings * int dimension

                // iterate through all the postings of the posting list
                for (Posting posting : posList) {
                    // Buffer not created
                    if (buffer_docid == null || buffer_termfreq == null)
                        return;

                    buffer_docid.putInt(posting.getDocId());         // write DocID
                    buffer_termfreq.putInt(posting.getTermFreq());   // write TermFrequency
//                    if(term.equals("0000"))
//                        printDebug("docId " + posting.getDocId() +  " termfreq " + posting.getTermFreq());
                    INDEX_OFFSET += INT_BYTES;
                }

                // store dictionary entry to disk
                dictElem.storeDictionaryElemIntoDisk(dictChannel, false);
            }

            System.out.println(dictionary.getTermToTermStat().size() + " terms stored in block " + (dictionaryBlockOffsets.size()-1));

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    // store one posting list of a term into the disk
    public static void storePostingListIntoDisk(ArrayList<Posting> pl, FileChannel termfreqChannel, FileChannel docidChannel) {

        //number of postings in the posting list
        int len = pl.size();

        // Create buffers for docid and termfreq
        try {
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, docidChannel.size(), (long) len*Integer.BYTES); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, termfreqChannel.size(), (long) len*Integer.BYTES); //from 0 to number of postings * int dimension

            for (Posting posting : pl) {
                bufferdocid.putInt(posting.getDocId());
                buffertermfreq.putInt(posting.getTermFreq());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // -------- end: functions to store into disk --------


    // -------- start: functions to read from disk --------

    // function to read all document table from disk and put it in memory (HashMap documentTable)
    public static void readDocumentTableFromDisk(boolean indexBuilding) throws IOException {
        System.out.println("Loading document table from disk...");

        try (
             RandomAccessFile docTableRaf = new RandomAccessFile(DOCTABLE_FILE, "r");
             FileChannel channel = docTableRaf.getChannel()
        ) {

            DocumentElement de = new DocumentElement();

            // for to read all DocumentElement stored into disk
            for (int i = 0; i < channel.size(); i += DOCELEM_SIZE) {
                de.readDocumentElementFromDisk(i, channel); // get the ith DocElem
                if(indexBuilding)
                    PartialIndexBuilder.documentTable.put(de.getDocid(), new DocumentElement(de.getDocno(), de.getDocid(), de.getDoclength()));
                else
                    QueryProcessor.documentTable.put(de.getDocid(), new DocumentElement(de.getDocno(), de.getDocid(), de.getDoclength()));
            }
        }
    }

    // function to read offset of the block from disk
    public static void readBlockOffsetsFromDisk(){

        System.out.println("\nLoading block offsets from disk...");

        if(!dictionaryBlockOffsets.isEmpty()) //control check
            dictionaryBlockOffsets.clear();

        try (
                RandomAccessFile raf = new RandomAccessFile(BLOCKOFFSETS_FILE, "rw");
                FileChannel channel = raf.getChannel()
        ) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0 , channel.size());

            if(buffer == null)      // Buffer not created
                return;

            // iterate through all files for #blocks times
            for(int i = 0; i < channel.size()/ LONG_BYTES; i++){
                dictionaryBlockOffsets.add(buffer.getLong());
                buffer.position((i+1)*LONG_BYTES); //skip to position of the data of the next block to read
                printDebug("OFFSET BLOCK " + i + ": " + dictionaryBlockOffsets.get(i));
            }

            System.out.println(dictionaryBlockOffsets.size() + " blocks loaded");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * function to read and return a posting list from disk
     *
     * @param offsetDocId       offset of the DocID
     * @param offsetTermFreq    offset of the Term Frequency
     * @param posting_size      size of the posting list
     * @param docidChannel      file where read DocID values
     * @param termfreqChannel   file where read Term Frequency values
     * @return the posting lists read from disk
     */
    public static ArrayList<Posting> readPostingListFromDisk(long offsetDocId, long offsetTermFreq, int posting_size, FileChannel docidChannel, FileChannel termfreqChannel) {

        ArrayList<Posting> pl = new ArrayList<>();

        try {
            MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, (long) posting_size * Integer.BYTES);
            MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, (long) posting_size * Integer.BYTES);

            //while nr of postings read are less than the number of postings to read (all postings of the term)
            for (int i = 0; i < posting_size; i++) {
                int docid = docidBuffer.getInt();           // read the DocID
                int termfreq = termfreqBuffer.getInt();     // read the TermFrequency
                pl.add(new Posting(docid, termfreq)); // add the posting to the posting list
            }
            return pl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

//    /**
//     * function to obtain the complete posting list (stored into disk) related the term passed by parameter
//     *
//     * @param term
//     * @return
//     */
//    public static ArrayList<Posting> getPostingListFromTerm(String term)
//    {
//        ArrayList<Posting> postingList = new ArrayList<>();     // contain the posting list of term
//        DictionaryElem de;
//
//       try (
//                // open complete files to read the postingList
//                RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
//                RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");
//                // FileChannel
//                FileChannel docIdChannel = docidFile.getChannel();
//                FileChannel termFreqChannel = termfreqFile.getChannel();
//        ) {
//            // check if the term is the term is present in the dictionary hashmap
//            if (dictionary.getTermToTermStat().containsKey(term))
//            {
//                de = dictionary.getTermToTermStat().get(term);      // take DictionaryElem related term
//                // take the postingList of term
//                postingList = readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf(),docIdChannel,termFreqChannel);
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return postingList;
//    }
    // -------- end: functions to read from disk --------

    /**
     * function to store posting list after compression into disk
     *
     * @param pl                posting list to store
     * @param docidChannel      file where store DocID values
     * @param termfreqChannel   file where store Term Frequency values
     * @return Term Frequency and DocID compressed length
     */

    public static int[] storeCompressedPostingIntoDisk(ArrayList<Posting> pl, FileChannel termfreqChannel, FileChannel docidChannel){

        ArrayList<Integer> tf = new ArrayList<>();
        ArrayList<Integer> docid  = new ArrayList<>();
        int[] length = new int[2];
        //number of postings in the posting list
        for(Posting ps : pl) {
            tf.add(ps.getTermFreq());
            docid.add(ps.getDocId());
        }

        byte[] compressedTf = Unary.integersCompression(tf);
        byte[] compressedDocId = VariableBytes.integersCompression(docid);
        // Create buffers for docid and termfreq
        try {
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, termfreqChannel.size(), compressedTf.length); //number of bytes of compressed tfs
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, docidChannel.size(), compressedDocId.length); //number of bytes of compressed docids

            buffertermfreq.put(compressedTf);
            bufferdocid.put(compressedDocId);

            length[0] = compressedTf.length;
            length[1] = compressedDocId.length;
            return length;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
    /**
     * function to store posting list after compression into disk
     *
     * @param offsetDocId       offset from where to start the read of the DocID values
     * @param offsetTermFreq    offset from where to start the read of the Term Frequency values
     * @param termFreqSize      size of the compressed Term Frequency values
     * @param docIdSize         size of the compressed DocID values
     * @param posting_size      posting list size
     * @param docidChannel      file where read DocID values
     * @param termfreqChannel   file where read Term Frequency values
     * @return termfreq and docid compressed length
     */
    public static ArrayList<Posting> readCompressedPostingListFromDisk(long offsetDocId, long offsetTermFreq, int termFreqSize, int docIdSize, int posting_size, FileChannel docidChannel, FileChannel termfreqChannel) {

        ArrayList<Posting> uncompressed = new ArrayList<>();
        byte[] docids = new byte[docIdSize];
        byte[] tf = new byte[termFreqSize];

        try {
            MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, docIdSize);
            MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, termFreqSize);

            termfreqBuffer.get(tf, 0, termFreqSize);
            docidBuffer.get(docids, 0, docIdSize );

            ArrayList<Integer> uncompressedTf = Unary.integersDecompression(tf, posting_size);
            ArrayList<Integer> uncompressedDocid = VariableBytes.integersDecompression(docids);
            for(int i = 0; i < posting_size; i++) {
                //System.out.println("docid: " + uncompressedDocid.get(i)  + " tf: " + uncompressedTf.get(i));
                uncompressed.add(new Posting(uncompressedDocid.get(i), uncompressedTf.get(i))); // add the posting to the posting list
            }
            return uncompressed;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }



// function to save docids  or tf posting list into file (in order to compare before and after compression)
    public static void saveDocsInFile(ArrayList<Integer> postings, boolean merge) throws FileNotFoundException {
        // ----------- debug file ---------------
        String tempFileName = (merge? "tempFile.txt" : "tempFile_out.txt");
        File outputf = new File(tempFileName);


        PrintWriter outputWriter = new PrintWriter(outputf);

        for(int i = 0; i < postings.size(); i++) {
            outputWriter.print(postings.get(i));
            outputWriter.print(" ");
    }

}

}
