package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.compression.Unary;
import it.unipi.dii.aide.mircv.Query;
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
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static it.unipi.dii.aide.mircv.utils.Logger.spimi_logger;


/**
 * This class handles the storage and retrieval of data structures used for document indexing.
 */
public final class DataStructureHandler {

    static  MappedByteBuffer buffer;


    // -------- start: functions to store into disk --------

    // function to store the whole document table into disk
    static void storeDocumentTableIntoDisk() {

        try {

            MappedByteBuffer buffer = docTable_channel.map(FileChannel.MapMode.READ_WRITE, docTable_channel.size(), (long) DOCELEM_SIZE * PartialIndexBuilder.documentTable.size());

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

        try {
            MappedByteBuffer buffer = blocks_channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) LONG_BYTES * dictionaryBlockOffsets.size()); //offset_size (size of dictionary offset) * number of blocks

            // Buffer not created
            if(buffer == null)
                return;

            // scan all block and for each one write offset into disk
            for (int i = 0; i < dictionaryBlockOffsets.size(); i++) {
                printDebug("OFFSET BLOCK " + i + ": " + dictionaryBlockOffsets.get(i));
                buffer.putLong(dictionaryBlockOffsets.get(i)); //store into file the dictionary offset of the i-th block
                if(debug)
                    appendStringToFile("Offset block " + i + ": " + dictionaryBlockOffsets.get(i), "blocks.txt");
            }

            System.out.println(dictionaryBlockOffsets.size() + " blocks stored");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // function to store Dictionary and Inverted Index into disk
    public static void storeIndexAndDictionaryIntoDisk() {

        try {
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

                // Create buffers for docid and termfreq
                MappedByteBuffer buffer_docid = partialDocId_channel.map(FileChannel.MapMode.READ_WRITE, partialDocId_channel.size(), (long) posList.size() * INT_BYTES); // from 0 to number of postings * int dimension
                MappedByteBuffer buffer_termfreq = partialTermFreq_channel.map(FileChannel.MapMode.READ_WRITE, partialTermFreq_channel.size(), (long) posList.size() * INT_BYTES); //from 0 to number of postings * int dimension

                // iterate through all the postings of the posting list
                for (Posting posting : posList) {
                    // Buffer not created
                    if (buffer_docid == null || buffer_termfreq == null)
                        return;

                    buffer_docid.putInt(posting.getDocId());         // write DocID
                    buffer_termfreq.putInt(posting.getTermFreq());   // write TermFrequency

                    if(debug) {
                        appendStringToFile(dictElem.getTerm() + ": " + posting, "spimi_pl.txt");
                        appendStringToFile(dictElem.getTerm() + ": " + posting.getDocId(), "spimi_docid.txt");
                        appendStringToFile(dictElem.getTerm() + ": " + posting.getTermFreq(), "spimi_tf.txt");
                    }

                    INDEX_OFFSET += INT_BYTES;
                }

//                if(log)
//                    spimi_logger.logInfo("Posting list size = " + posList.size());

                // store dictionary entry to disk
                dictElem.storeDictionaryElemIntoDisk();
            }
            System.out.println(dictionary.getTermToTermStat().size() + " terms stored in block " + (dictionaryBlockOffsets.size()-1));

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    // store one posting list of a term into the disk
    public static void storePostingListIntoDisk(ArrayList<Posting> pl) {

        //number of postings in the posting list
        int len = pl.size();

        // Create buffers for docid and termfreq
        try {
            MappedByteBuffer bufferdocid = docId_channel.map(FileChannel.MapMode.READ_WRITE, docId_channel.size(), (long) len*Integer.BYTES); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termFreq_channel.map(FileChannel.MapMode.READ_WRITE, termFreq_channel.size(), (long) len*Integer.BYTES); //from 0 to number of postings * int dimension

            for (Posting posting : pl) {
                bufferdocid.putInt(posting.getDocId());
                buffertermfreq.putInt(posting.getTermFreq());
                if(debug) {
                    appendStringToFile(posting.toString(), "merge_pl.txt");
                    appendStringToFile(String.valueOf(posting.getDocId()), "merge_docid.txt");
                    appendStringToFile(String.valueOf(posting.getTermFreq()), "merge_tf.txt");
                }
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

        try (RandomAccessFile docTableFile = new RandomAccessFile(DOCTABLE_FILE, "r")) {

            docTable_channel = docTableFile.getChannel();

            DocumentElement de = new DocumentElement();

            // for to read all DocumentElement stored into disk
            for (int i = 0; i < docTable_channel.size(); i += DOCELEM_SIZE) {
                de.readDocumentElementFromDisk(i, docTable_channel); // get the ith DocElem
                if (indexBuilding)
                    PartialIndexBuilder.documentTable.put(de.getDocid(), new DocumentElement(de.getDocno(), de.getDocid(), de.getDoclength()));
                else
                    Query.documentTable.put(de.getDocid(), new DocumentElement(de.getDocno(), de.getDocid(), de.getDoclength()));
            }

        }catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    // function to read offset of the block from disk
    public static void readBlockOffsetsFromDisk(){

        System.out.println("\nLoading block offsets from disk...");

        if(!dictionaryBlockOffsets.isEmpty()) //control check
            dictionaryBlockOffsets.clear();

        try {
            MappedByteBuffer buffer = blocks_channel.map(FileChannel.MapMode.READ_WRITE, 0 , blocks_channel.size());

            if(buffer == null)      // Buffer not created
                return;

            // iterate through all files for #blocks times
            for(int i = 0; i < blocks_channel.size()/ LONG_BYTES; i++){
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
     * @return the posting lists read from disk
     */
    public static ArrayList<Posting> readPostingListFromDisk(long offsetDocId, long offsetTermFreq, int posting_size) {

        ArrayList<Posting> pl = new ArrayList<>();

        MappedByteBuffer docidBuffer;
        MappedByteBuffer termfreqBuffer;

        try{

            if(Flags.considerSkippingBytes()){
                docidBuffer = docId_channel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, (long) posting_size * Integer.BYTES);
                termfreqBuffer = termFreq_channel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, (long) posting_size * Integer.BYTES);
            } else {
                docidBuffer = partialDocId_channel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, (long) posting_size * Integer.BYTES);
                termfreqBuffer = partialTermFreq_channel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, (long) posting_size * Integer.BYTES);
            }

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

    // -------- end: functions to read from disk --------

    /**
     * function to store posting list after compression into disk
     *
     * @param pl                posting list to store
     * @return Term Frequency and DocID compressed length
     */

    public static int[] storeCompressedPostingIntoDisk(ArrayList<Posting> pl){

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
            MappedByteBuffer buffertermfreq = termFreq_channel.map(FileChannel.MapMode.READ_WRITE, termFreq_channel.size(), compressedTf.length); //number of bytes of compressed tfs
            MappedByteBuffer bufferdocid = docId_channel.map(FileChannel.MapMode.READ_WRITE, docId_channel.size(), compressedDocId.length); //number of bytes of compressed docids

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
     * @return termfreq and docid compressed length
     */
    public static ArrayList<Posting> readCompressedPostingListFromDisk(long offsetDocId, long offsetTermFreq, int termFreqSize, int docIdSize, int posting_size) {

        ArrayList<Posting> uncompressed = new ArrayList<>();
        byte[] docids = new byte[docIdSize];
        byte[] tf = new byte[termFreqSize];

        try {
            MappedByteBuffer docidBuffer = docId_channel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, docIdSize);
            MappedByteBuffer termfreqBuffer = termFreq_channel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, termFreqSize);

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

}
