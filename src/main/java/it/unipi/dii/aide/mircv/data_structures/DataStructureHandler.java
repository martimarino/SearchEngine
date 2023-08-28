package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.Flag;
import it.unipi.dii.aide.mircv.Main;
import it.unipi.dii.aide.mircv.TextProcessor;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class handles the storage and retrieval of data structures used for document indexing.
 */
public class DataStructureHandler {

    // Constants for file paths
    public static final String DOCUMENT_FILE = "src/main/resources/document.txt"; // file in which is stored the document table
    public static final String VOCABULARY_FILE = "src/main/resources/vocabulary.txt"; // file in which is stored the vocabulary
    public static final String PARTIAL_VOCABULARY_FILE = "src/main/resources/partial_vocabulary.txt"; // file in which is stored the vocabulary in blocks
    public static final String BLOCK_FILE = "src/main/resources/blocks.txt"; // file containing the offset of each vocabulary block
    public static final String FLAGS_FILE = "src/main/resources/flags"; // file in which flags are stored
    public static final String PARTIAL_DOCID_FILE = "src/main/resources/docid.txt";  // file containing the docId (element of posting list) for each block
    public static final String PARTIAL_TERMFREQ_FILE = "src/main/resources/termfreq.txt";   // file containing the TermFrequency (element of posting list) for each block
    public static final String DOCID_FILE = "src/main/resources/mergedDocId.txt";   // file containing the docId of the InvertedIndex merged
    public static final String TERMFREQ_FILE = "src/main/resources/mergedTermFreq.txt";   // file containing the termFreq of the InvertedIndex merged

    public static final int DOCNO_DIM = 10;             // Length of docno (in bytes)
    public static final int TERM_DIM = 20;              // Length of a term (in bytes)
    public static final int BLOCK_SIZE = Long.BYTES;    // Length of block (in bytes)

    private static int N_POSTINGS = 0;                  // Number of partial postings to save in the file
    private static long DICTIONARY_OFFSET = 0;          // Offset of the terms in the dictionary
    private static long INDEX_OFFSET = 0;               // Offset of the termfreq and docid in index

    // Data structures initialization
    private static DocumentTable dt = new DocumentTable();   // we can not instantiate it while building it, we can do it only during read from file
    static final Dictionary dictionary = new Dictionary();
    private static final InvertedIndex invertedIndex = new InvertedIndex();
    private static HashMap<String, ArrayList<Posting>> invIndex = new HashMap<>();

    public static ArrayList<Long> dictionaryBlocks = new ArrayList<>();    // Offsets of the dictionary blocks

    // variable that indicates after how many iterations to make a control printout (used in various methods)
    public static int printInterval = 1000;
    // variable that stipulates the behaviour for control printouts. If false there will be no printouts, if true there will be all printouts.
    public static boolean verbose = false;

    /**
     * Initializes data structures and fills them from the input collection.
     *
     * @throws IOException if there is an error reading the collection.
     */

    /**
     * Implements the SPIMI algorithm for indexing large collections.
     */
    public static void SPIMIalgorithm() {

        long memoryAvailable = Runtime.getRuntime().maxMemory() * 80 / 100;
        int docCounter = 1;
        int termCounter = 1;
        long startTime, endTime;    // variables to show execution time

        dictionaryBlocks = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.COLLECTION_PATH), StandardCharsets.UTF_8))) {

            String record;

            startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
            // while to scan all documents in the collection
            while ((record = br.readLine()) != null) {
                if(record.isBlank()) // empty string or composed by whitespace characters
                    continue;

/*                if (docCounter % printInterval == 0)
                    System.out.println("MEMORY AVAILABLE : " + memoryAvailable);*/

                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text

                String docno = preprocessed.remove(0);      // get the DocNO of the current document
                if (preprocessed.isEmpty()) {
                    continue;              // Empty documents, skip to next while iteration
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());
                dt.getDocIdToDocElem().put(docCounter, de);
                docCounter++;              // update DocID counter

                for (String term : preprocessed) {

                    // Dictionary build
                    if(term.length() > TERM_DIM)
                        term = term.substring(0,TERM_DIM);

                    DictionaryElem dictElem = dictionary.getOrCreateTerm(term, termCounter);
                    termCounter++;         // update TermID counter

                    // Build inverted index
                    // "addTerm" add posting in inverted index and return true if term is in a new doc -> update df
                 /*   if (invertedIndex.addTerm(term, docCounter, 0)) {
                        dictElem.incDf();                // increment Document Frequency of the term in the dictionary
                    }*/

                    if(addTerm(term, docCounter, 0))
                        dictElem.incDf();

                    N_POSTINGS++;
                    //System.out.println("*** NPOSTINGS: " + npostings + "***");
                }

                // Print memory usage every 500 documents
                if ((docCounter % printInterval == 0) && verbose) {
                    System.out.println("NUM DOC: " + docCounter);
                    //System.out.println("TOT MEMORY: " + Runtime.getRuntime().totalMemory() + " - MEM AVAILABLE: " + memoryAvailable);
                }

                if(Runtime.getRuntime().totalMemory() > memoryAvailable) {
                    System.out.println("********** Memory full **********");
                    //store index and dictionary to disk
                    storeIndexAndVocabularyIntoDisk();
                    DataStructureHandler.storeDocumentTableIntoDisk(); // store document table one document at a time for each block
                    freeMemory();
                    System.gc();
                    System.out.println("********** Free memory **********");
                    N_POSTINGS = 0; // new partial index

                }
            }
            endTime = System.currentTimeMillis();           // end time of SPIMI algorithm
            System.out.println("\nSPIMI Algorithm done in(s): " + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);

            // store blocks into disk
            startTime = System.currentTimeMillis();         // start time to store block
            storeBlocksIntoDisk();
            endTime = System.currentTimeMillis();           // end time of store block
            System.out.println("\nStore block (end of SPIMI Algorithm) done\nin(ms): " + (endTime-startTime) + "in(s): " + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);

            // merge blocks into disk
            startTime = System.currentTimeMillis();         // start time to merge blocks
            IndexMerger.mergeBlocks();
            endTime = System.currentTimeMillis();           // end time of merge blocks
            System.out.println("\nMerge block (end of SPIMI Algorithm) done\nin(s): " + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void storeDocumentTableIntoDisk() {


        try (RandomAccessFile raf = new RandomAccessFile(DOCUMENT_FILE, "rw");
             FileChannel channel = raf.getChannel()) {

            int docsize = 4 + DOCNO_DIM + 4; // Size in bytes of docid, docno, and doclength
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), docsize*dt.getDocIdToDocElem().size());

            // Buffer not created
            if(buffer == null)
                return;

            for(DocumentElement de: dt.getDocIdToDocElem().values()) {
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

    public static boolean addTerm(String term, int docId, int tf) {
        // Initialize term frequency to 1 if tf is not provided (tf = 0 during index construction)
        int termFreq = (tf != 0) ? tf : 1;

        // Get or create the PostingList associated with the term
        if(!invIndex.containsKey(term))
            invIndex.put(term, new ArrayList<Posting>());

        int size = invIndex.get(term).size();
        // Check if the posting list is empty or if the last posting is for a different document
        if (invIndex.get(term).isEmpty() || invIndex.get(term).get(size - 1).getDocId() != docId) {
            // Add a new posting for the current document
            invIndex.get(term).add(new Posting(docId, termFreq));

            // Print term frequency and term frequency in the current posting (only during index construction)
            if (tf != 0) {
                System.out.println("TF: " + tf + " TERMFREQ: " + termFreq);
            }

            return true; // Increment df only if it's a new document
        } else {
            // Increment the term frequency for the current document
            int lastTermFreq = invIndex.get(term).get(size - 1).getTermFreq();
            invIndex.get(term).get(size - 1).setTermFreq(lastTermFreq + 1);
            return false; // No need to increment df
        }
    }


    private static void storeBlocksIntoDisk() {

        try (RandomAccessFile raf = new RandomAccessFile(BLOCK_FILE, "rw");
             FileChannel channel = raf.getChannel()) {

            int offset_size = Long.BYTES; // Size in bytes of vocabulary offset in inverted index file for each block
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) offset_size *dictionaryBlocks.size()); //offset_size (size of dictionary offset) * number of blocks

            // Buffer not created
            if(buffer == null)
                return;

            for(int i = 0; i < dictionaryBlocks.size(); i++)
            {
                System.out.println("BLOCK: " + i + ": " + dictionaryBlocks.get(i));
                buffer.putLong(dictionaryBlocks.get(i)); //store into file the dictionary offset of the i-th block
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // -- start of store and get functions --
/*
    public static void storeCollectionIntoDisk(){

    }*/

    public static void storeFlagsIntoDisk() throws IOException {

        try ( RandomAccessFile Flags_raf  = new RandomAccessFile(new File(FLAGS_FILE),"rw");) {
            ByteBuffer FlagsBuffer = ByteBuffer.allocate(12);
            Flags_raf.getChannel().position(0);

            FlagsBuffer.putInt(Flag.isSwsEnabled() ? 1 : 0);
            FlagsBuffer.putInt(Flag.isScoringEnabled() ? 1 : 0);
            FlagsBuffer.putInt(Flag.isCompressionEnabled() ? 1 : 0);

            FlagsBuffer = ByteBuffer.wrap(FlagsBuffer.array());

            while (FlagsBuffer.hasRemaining())
                Flags_raf.getChannel().write(FlagsBuffer);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void storeDictionaryIntoDisk(DictionaryElem dictElem, FileChannel channel){
        try {
            int vocsize = TERM_DIM + 4 + 4 + 4 + 8 + 8; // Size in bytes of term, df, cf, termId, offsets
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), vocsize);
            // Buffer not created
            if(buffer == null)
                return;
            //allocate 20bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(TERM_DIM);
            //put every char into charbuffer
            for(int i = 0; i < dictElem.getTerm().length(); i++)
                charBuffer.put(i, dictElem.getTerm().charAt(i));
            // write docno, docid and doclength into document file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
            buffer.putInt(dictElem.getDf());
            buffer.putInt(dictElem.getCf());
            buffer.putInt(dictElem.getTermId());
            buffer.putLong(DICTIONARY_OFFSET);
            buffer.putLong(DICTIONARY_OFFSET);
            DICTIONARY_OFFSET += vocsize;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void storeIndexAndVocabularyIntoDisk() {

        final int INT_SIZE = 4;
        int vocsize = TERM_DIM + 4 + 4 + 4 + 8 + 8; // Size in bytes of term, df, cf, termId, offsets


        try (
                RandomAccessFile docidFile = new RandomAccessFile(PARTIAL_DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(PARTIAL_TERMFREQ_FILE, "rw");
                FileChannel docidChannel = docidFile.getChannel();
                FileChannel termfreqChannel = termfreqFile.getChannel();
                RandomAccessFile raf = new RandomAccessFile(PARTIAL_VOCABULARY_FILE, "rw");
                FileChannel channel = raf.getChannel()
        ) {
            // Create buffers for docid and termfreq
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) N_POSTINGS * INT_SIZE); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) N_POSTINGS * INT_SIZE); //from 0 to number of postings * int dimension

            // Sort the dictionary lexicographically
            dictionary.sort();
            dictionaryBlocks.add(DICTIONARY_OFFSET);// update of the offset of the block for the dictionary file

            // iterate through all the terms of the dictionary ordered
            for (String term : dictionary.getTermToTermStat().keySet()) {
                //get posting list of the term
                ArrayList<Posting> posList = invIndex.get(term);
                //create dictionary element for the term
                DictionaryElem dictElem = dictionary.getTermStat(term);
                dictElem.setOffsetTermFreq(INDEX_OFFSET);
                dictElem.setOffsetDocId(INDEX_OFFSET);
                //iterate through all the postings of the posting list
                for (Posting posting : posList) {
                    // Buffer not created
                    if (bufferdocid == null || buffertermfreq == null)
                        return;

                    // Write DocID and TermFreq to buffers
                    //System.out.println("OF+DOCID: " + posting.getDocId() + " TERMFREQ: " + posting.getTermFreq());
                    bufferdocid.putInt(posting.getDocId());         // write DocID
                    buffertermfreq.putInt(posting.getTermFreq());   // write TermFrequency
                    INDEX_OFFSET += INT_SIZE;
                }
                // store dictionary entry to disk
                //storeDictionaryIntoDisk(dictElem, PARTIAL_VOCABULARY_FILE);
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), vocsize);

                if (verbose)
                    System.out.println("TERM DICT: " + term);       // control print of the term
                //allocate 20bytes for docno
                CharBuffer charBuffer = CharBuffer.allocate(TERM_DIM);
                //put every char into charbuffer
                for(int i = 0; i < term.length(); i++)
                    charBuffer.put(i, term.charAt(i));
                // write docno, docid and doclength into document file
                buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
                buffer.putInt(dictElem.getDf());
                buffer.putInt(dictElem.getCf());
                buffer.putInt(dictElem.getTermId());
                buffer.putLong(DICTIONARY_OFFSET);
                buffer.putLong(DICTIONARY_OFFSET);
                DICTIONARY_OFFSET += vocsize;

            }

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static void getCollectionFromDisk() {

    }

    public static void getFlagsFromDisk() {

    }

    /**
     *
     */
    public static void getBlocksFromDisk(){
        if(!dictionaryBlocks.isEmpty()) //control check
            dictionaryBlocks.clear();

        try (FileChannel channel = new RandomAccessFile(BLOCK_FILE, "rw").getChannel()) {
            int offset_size = Long.BYTES; // Size in bytes of the offset of the partial vocabulary in the blocks
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0 , channel.size());
            // Buffer not created
            if(buffer == null)
                return;
            // iterate trough all the file for number of blocks times
           for(int i = 0; i < channel.size()/offset_size; i++){
               dictionaryBlocks.add(buffer.getLong());
               buffer.position((i+1)*Long.BYTES); //skip to position of the data of the next block to read
           }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

   /**
    @param start offset of the document reading from document file
    **/
    public static DocumentElement getDocumentIndexFromDisk(int start) {

        DocumentElement de = new DocumentElement();
        try (FileChannel channel = new RandomAccessFile(DOCUMENT_FILE, "rw").getChannel()) {
            int docsize = 4 + DOCNO_DIM + 4; // Size in bytes of docid, docno, and doclength
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, start, docsize);

            // Buffer not created
            if(buffer == null)
                return null;

            CharBuffer.allocate(DOCNO_DIM); //allocate a charbuffer of the dimension reserved to docno
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            if(charBuffer.toString().split("\0").length == 0)
                return null;

            de.setDocno(charBuffer.toString().split("\0")[0]); //split using end string character
            buffer.position(DOCNO_DIM); //skip docno
            de.setDoclength(buffer.getInt());
            de.setDocid(buffer.getInt());

            // print of the document element fields taken from the disk
            if((start % printInterval == 0) && verbose)
                System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());

            return de;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static DictionaryElem getDictionaryElemFromDisk(long start, FileChannel channel){

        long vocsize =  TERM_DIM + 4 + 4 + 4 + 8 + 8; // Size in bytes of term, df, cf, termId, offset

        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start,  vocsize); // get first term of the block
            DictionaryElem le = new DictionaryElem();           // create new DictionaryElem
            CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to docno
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            if(!(charBuffer.toString().split("\0").length == 0))
                le.setTerm(charBuffer.toString().split("\0")[0]);

            buffer.position(TERM_DIM);
            le.setCf(buffer.getInt());
            le.setDf(buffer.getInt());
            le.setTermId(buffer.getInt());
            le.setOffsetTermFreq(buffer.getLong());
            le.setOffsetDocId(buffer.getLong());

            // print of the dictionary element fields taken from the disk
            if (verbose)
                System.out.println("TERM: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " TERMID: " + le.getTermId() + " OFFSET: " + le.getOffsetDocId());
            return le;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Dictionary getDictionaryFromDisk(){

        long vocsize = TERM_DIM + 4 + 4 + 4 + 8 + 8; // Size in bytes of term, df, cf, termId, offset
        long position = 0;
        Dictionary d = new Dictionary();

        MappedByteBuffer buffer = null; // get first term of the block
        try (FileChannel channel = new RandomAccessFile(VOCABULARY_FILE, "rw").getChannel()) {
            long len = channel.size();
            System.out.println(len);
            while(position < len) {
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, position + vocsize);
                position += vocsize;

                DictionaryElem le = new DictionaryElem();           // create new DictionaryElem

                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to docno
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                if(charBuffer.toString().split("\0").length == 0)
                    return null;
                String term = charBuffer.toString().split("\0")[0];
                le.setTerm(term); //split using end string character
                buffer.position(TERM_DIM); //skip docno
                le.setCf(buffer.getInt());
                le.setDf(buffer.getInt());
                le.setTermId(buffer.getInt());
                le.setOffsetTermFreq(buffer.getLong());
                le.setOffsetDocId(buffer.getLong());
                d.getTermToTermStat().put(term, le);

                // print of the dictionary element fields taken from the disk
                if((position % printInterval == 0) && verbose)
                    System.out.println("TERM: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " TERMID: " + le.getTermId() + " OFFSET: " + le.getOffsetDocId());
            }
            return dictionary;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static PostingList readIndexElemFromDisk(long offsetDocId, long offsetTermFreq, String term, int posting_size, FileChannel docidChannel, FileChannel termfreqChannel) {

        PostingList pl = new PostingList();
        pl.setPostings(new ArrayList<>());

            try {
                for (int i = 0; i < posting_size; i++) {
                    MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_WRITE, offsetDocId, Integer.BYTES);
                    MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, offsetTermFreq, Integer.BYTES);

                    //while nr of postings read are less than the number of postings to read (all postings of the term)
                    //System.out.println("TERM: " + term + " TERMFREQ: " + termfreqBuffer.getInt() + " DOCID: " + docidBuffer.getInt());
                    int docid = docidBuffer.getInt();           // read the DocID
                    int termfreq = termfreqBuffer.getInt();     // read the TermFrequency
                    //System.out.println("TERM: " + term + " TERMFREQ: " + termfreq + " DOCID: " + docid);
                    pl.addPosting(new Posting(docid, termfreq)); // add the posting to the posting list
                    //System.out.println(String.format("TERM: %20s - TERMFREQ: %8s - DOCID: %10s", term, termfreq, docid));
                }
                return pl;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
    }
    //store one posting list of a term into the disk
    public static void storePostingListToDisk(PostingList pl, FileChannel termfreqChannel, FileChannel docidChannel) {

        //number of postings in the posting list
        int len = pl.getPostings().size();
        // Create buffers for docid and termfreq
        try {
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, docidChannel.size(), len* Integer.BYTES); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, termfreqChannel.size(), len* Integer.BYTES); //from 0 to number of postings * int dimension
            for (Posting posting : pl.getPostings()) {
                bufferdocid.putInt(posting.getDocId());
                buffertermfreq.putInt(posting.getTermFreq());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // method to free memory by deleting the information in document table, dictionary,and inverted index
    public static void freeMemory(){
        dt.getDocIdToDocElem().clear();
        dictionary.getTermToTermStat().clear();
        invIndex.clear();
    }

}
