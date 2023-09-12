package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.Flag;
import it.unipi.dii.aide.mircv.TextProcessor;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.data_structures.DictionaryElem.*;
import static it.unipi.dii.aide.mircv.data_structures.DocumentElement.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

/**
 * This class handles the storage and retrieval of data structures used for document indexing.
 */
public class DataStructureHandler {

    // Data structures initialization
    private static final HashMap<Integer, DocumentElement> documentTable = new HashMap<>();     // hash table DocID to related DocElement
    private static final Dictionary dictionary = new Dictionary();
    private static final HashMap<String, ArrayList<Posting>> invertedIndex = new HashMap<>();

    public static ArrayList<Long> dictionaryBlockOffsets = new ArrayList<>();    // Offsets of the dictionary blocks
    public static CollectionStatistics collection = new CollectionStatistics();


    /**
     * Implements the SPIMI algorithm for indexing large collections.
     */
    public static void SPIMIalgorithm() {

        long memoryAvailable = (long) (Runtime.getRuntime().maxMemory() * MEMORY_THRESHOLD);
        int docCounter = 1;
        int termCounter = 1;        //for term id
        int totDocLen = 0;

        try (
                BufferedReader buffer_collection = new BufferedReader(new InputStreamReader(new FileInputStream(COLLECTION_PATH), StandardCharsets.UTF_8));
        ) {

            String record;

            // scan all documents in the collection
            while ((record = buffer_collection.readLine()) != null) {

                int separator = record.indexOf("\t");
                // malformed line, no \t
                if (record.isBlank() || separator == -1) { // empty string or composed by whitespace characters or malformed
                    continue;
                }

                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text
                String docno = preprocessed.remove(0);      // get the DocNO of the current document

                if (preprocessed.isEmpty() || (preprocessed.size() == 1 && preprocessed.get(0).equals("")))  {
                    continue;              // Empty documents, skip to next while iteration
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());
                documentTable.put(docCounter, de);
                docCounter++;              // update DocID counter
                totDocLen += preprocessed.size();


                for (String term : preprocessed) {

                    // Dictionary build
                    if(term.length() > TERM_DIM)
                        term = term.substring(0,TERM_DIM);

                    DictionaryElem dictElem = dictionary.getOrCreateTerm(term, termCounter);

                    termCounter++;         // update TermID counter

                    if(addTerm(term, docCounter, 0)) {
                        dictElem.addDf(1);
                    }
                    dictElem.addCf(1);

                    N_POSTINGS++;
                }

                if(Runtime.getRuntime().totalMemory() > memoryAvailable) {
                    System.out.println("********** Memory full **********");
                    //store index and dictionary to disk
                    storeIndexAndDictionaryIntoDisk();
                    storeDocumentTableIntoDisk(); // store document table one document at a time for each block
                    freeMemory();
                    System.gc();
                    System.out.println("********** Free memory **********");
                    N_POSTINGS = 0; // new partial index
                }
            }

            storeBlockOffsetsIntoDisk();

            collection.setnDocs(docCounter);
            collection.setTotDocLen(totDocLen);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean addTerm(String term, int docId, int tf) {
        // Initialize term frequency to 1 if tf is not provided (tf = 0 during index construction)
        int termFreq = (tf != 0) ? tf : 1;

        // Get or create the PostingList associated with the term
        if(!invertedIndex.containsKey(term))
            invertedIndex.put(term, new ArrayList<>());

        int size = invertedIndex.get(term).size();


        // Check if the posting list is empty or if the last posting is for a different document
        if (invertedIndex.get(term).isEmpty() || invertedIndex.get(term).get(size - 1).getDocId() != docId) {
            // Add a new posting for the current document
            invertedIndex.get(term).add(new Posting(docId, termFreq));

            // Print term frequency and term frequency in the current posting (only during index construction)
            if (tf != 0 && verbose) {
                System.out.println("TF: " + tf + " TERMFREQ: " + termFreq);
            }

            return true; // Increment df only if it's a new document
        } else {
            // Increment the term frequency for the current document
            int lastTermFreq = invertedIndex.get(term).get(size - 1).getTermFreq();
            invertedIndex.get(term).get(size - 1).setTermFreq(lastTermFreq + 1);
            return false; // No need to increment df
        }
    }



    // -- start of store functions --

    public static void storeFlagsIntoDisk() {

        System.out.println("Storing flags into disk...");

        try (
                RandomAccessFile Flags_raf  = new RandomAccessFile(FLAGS_FILE,"rw");
                FileChannel channel = Flags_raf.getChannel();
        ) {
            MappedByteBuffer FlagsBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (INT_BYTES * 3));

            FlagsBuffer.putInt(Flag.isSwsEnabled() ? 1 : 0);
            FlagsBuffer.putInt(Flag.isScoringEnabled() ? 1 : 0);
            FlagsBuffer.putInt(Flag.isCompressionEnabled() ? 1 : 0);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static void storeDocumentTableIntoDisk() {


        try (RandomAccessFile raf = new RandomAccessFile(DOCTABLE_FILE, "rw");
             FileChannel channel = raf.getChannel()) {

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), (long) DOCELEM_SIZE *documentTable.size());

            // Buffer not created
            if(buffer == null)
                return;

            for(DocumentElement de: documentTable.values()) {
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

    private static void storeBlockOffsetsIntoDisk() {

        System.out.println("\nStoring block offsets into disk...");

        try (
                RandomAccessFile raf = new RandomAccessFile(BLOCKOFFSETS_FILE, "rw");
                FileChannel channel = raf.getChannel()
        ) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) LONG_BYTES * dictionaryBlockOffsets.size()); //offset_size (size of dictionary offset) * number of blocks

            // Buffer not created
            if(buffer == null)
                return;

            for (int i = 0; i < dictionaryBlockOffsets.size(); i++) {
                if(verbose)
                    System.out.println("OFFSET BLOCK " + i + ": " + dictionaryBlockOffsets.get(i));
                buffer.putLong(dictionaryBlockOffsets.get(i)); //store into file the dictionary offset of the i-th block
            }

            System.out.println(dictionaryBlockOffsets.size() + " blocks stored");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void storeDictionaryElemIntoDisk(DictionaryElem dictElem, FileChannel channel){
        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), DICT_ELEM_SIZE);

            // Buffer not created
            if(buffer == null)
                return;
            //allocate bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(TERM_DIM);
            //put every char into charbuffer
            for(int i = 0; i < dictElem.getTerm().length(); i++)
                charBuffer.put(i, dictElem.getTerm().charAt(i));
            // write docno, docid and doclength into document file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
            buffer.putInt(dictElem.getDf());
            buffer.putInt(dictElem.getCf());
            buffer.putInt(dictElem.getTermId());
            buffer.putLong(PARTIAL_DICTIONARY_OFFSET);
            buffer.putLong(PARTIAL_DICTIONARY_OFFSET);
            PARTIAL_DICTIONARY_OFFSET += DICT_ELEM_SIZE;

//            if(IndexMerger.i % 1000 == 0) System.out.println("storeDictionaryElemIntoDisk: " + dictElem);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void storeIndexAndDictionaryIntoDisk() {

        try (
                RandomAccessFile docidFile = new RandomAccessFile(PARTIAL_DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(PARTIAL_TERMFREQ_FILE, "rw");
                RandomAccessFile dictFile = new RandomAccessFile(PARTIAL_DICTIONARY_FILE, "rw");
                FileChannel docidChannel = docidFile.getChannel();
                FileChannel termfreqChannel = termfreqFile.getChannel();
                FileChannel dictChannel = dictFile.getChannel()
        ) {
            // Create buffers for docid and termfreq
            MappedByteBuffer buffer_docid = docidChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) N_POSTINGS * INT_BYTES); // from 0 to number of postings * int dimension
            MappedByteBuffer buffer_termfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) N_POSTINGS * INT_BYTES); //from 0 to number of postings * int dimension

            // Sort the dictionary lexicographically
            dictionary.sort();
            dictionaryBlockOffsets.add(PARTIAL_DICTIONARY_OFFSET);// update of the offset of the block for the dictionary file

            // iterate through all the terms of the dictionary ordered
            for (String term : dictionary.getTermToTermStat().keySet()) {
                //get posting list of the term
                ArrayList<Posting> posList = invertedIndex.get(term);
                //create dictionary element for the term
                DictionaryElem dictElem = dictionary.getTermStat(term);
                dictElem.setOffsetTermFreq(INDEX_OFFSET);
                dictElem.setOffsetDocId(INDEX_OFFSET);

                //iterate through all the postings of the posting list
                for (Posting posting : posList) {
                    // Buffer not created
                    if (buffer_docid == null || buffer_termfreq == null)
                        return;

                    buffer_docid.putInt(posting.getDocId());         // write DocID
                    buffer_termfreq.putInt(posting.getTermFreq());   // write TermFrequency
                    INDEX_OFFSET += INT_BYTES;
                }

                // store dictionary entry to disk
                storeDictionaryElemIntoDisk(dictElem, dictChannel);

            }

            System.out.println(dictionary.getTermToTermStat().size() + " terms stored in block " + dictionaryBlockOffsets.size());

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    //store one posting list of a term into the disk
    public static void storePostingListIntoDisk(PostingList pl, FileChannel termfreqChannel, FileChannel docidChannel) {

        //number of postings in the posting list
        int len = pl.getPostings().size();
        // Create buffers for docid and termfreq
        try {
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, docidChannel.size(), (long) len * Integer.BYTES); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, termfreqChannel.size(), (long) len * Integer.BYTES); //from 0 to number of postings * int dimension

            for (Posting posting : pl.getPostings()) {
                bufferdocid.putInt(posting.getDocId());
                buffertermfreq.putInt(posting.getTermFreq());
            }

//            if(IndexMerger.i % 1000 == 0) System.out.println("storePostingListIntoDisk: " + pl);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // -- start of get functions --

    public static void readFlagsFromDisk() {

        System.out.println("Loading flags from disk...");

        try (RandomAccessFile flagsRaf = new RandomAccessFile(new File(FLAGS_FILE), "rw")) {
            ByteBuffer flagsBuffer = ByteBuffer.allocate(12);
            flagsRaf.getChannel().position(0);

            // Read flag values from file
            flagsRaf.getChannel().read(flagsBuffer);

            // Move to the beginning of file for reading
            flagsBuffer.rewind();

            // Get flag values from buffer
            int isSwsEnabled = flagsBuffer.getInt();
            int isScoringEnabled = flagsBuffer.getInt();
            int isCompressionEnabled = flagsBuffer.getInt();

            // Set flag values with values read
            Flag.setSws(isSwsEnabled == 1);
            Flag.setScoring(isScoringEnabled == 1);
            Flag.setCompression(isCompressionEnabled == 1);

            if(verbose) System.out.println("*** Flags read -> sws: " + isSwsEnabled + ", scoring: " + isScoringEnabled + ", compression: " + isCompressionEnabled);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * @param start   offset of the document reading from document file
     */
    public static DocumentElement readDocumentElementFromDisk(int start, FileChannel channel) throws IOException {

        DocumentElement de = new DocumentElement();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, DOCELEM_SIZE);

        // Buffer not created
        if(buffer == null)
            return null;

        CharBuffer.allocate(DOCNO_DIM); //allocate a charbuffer of the dimension reserved to docno
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

        if(charBuffer.toString().split("\0").length == 0)
            return null;

        de.setDocno(charBuffer.toString().split("\0")[0]); //split using end string character
        buffer.position(DOCNO_DIM); //skip docno
        de.setDocid(buffer.getInt());
        de.setDoclength(buffer.getInt());

        // print of the document element fields taken from the disk
        if((start % printInterval == 0) && verbose)
            System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());

        return de;
    }

    public static void readDocumentTableFromDisk() throws IOException {
        System.out.println("Loading document table from disk...");
        try (RandomAccessFile docTableRaf = new RandomAccessFile(DOCTABLE_FILE, "r");
             FileChannel channel = docTableRaf.getChannel()) {

            // for to read all DocumentElement stored into disk
            for (int i = 0; i < channel.size(); i += DOCELEM_SIZE) {
                DocumentElement de = DataStructureHandler.readDocumentElementFromDisk(i, channel); // get the ith DocElem
                if (de != null)
                    documentTable.put(de.getDocid(), new DocumentElement(de.getDocno(), de.getDocid(), de.getDoclength()));
            }
        }
    }

    public static void readBlockOffsetsFromDisk(){

        System.out.println("\nLoading block offsets from disk...");

        if(!dictionaryBlockOffsets.isEmpty()) //control check
            dictionaryBlockOffsets.clear();

        try (FileChannel channel = new RandomAccessFile(BLOCKOFFSETS_FILE, "rw").getChannel()) {

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0 , channel.size());

            if(buffer == null)      // Buffer not created
                return;

            // iterate through all files for #blocks times
            for(int i = 0; i < channel.size()/ LONG_BYTES; i++){
                dictionaryBlockOffsets.add(buffer.getLong());
                buffer.position((i+1)*LONG_BYTES); //skip to position of the data of the next block to read
                //if(verbose)
                    System.out.println("OFFSET BLOCK " + i + ": " + dictionaryBlockOffsets.get(i));
            }

            System.out.println(dictionaryBlockOffsets.size() + " blocks loaded");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static DictionaryElem readDictionaryElemFromDisk(long start, FileChannel channel){

        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start,  DICT_ELEM_SIZE); // get first term of the block
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

            if(IndexMerger.i % 1000 == 0) System.out.println("readDictionaryElemFromDisk: " + le);


//            // print of the dictionary element fields taken from the disk
//            if (verbose && (printInterval % start == 0))
//                System.out.println("Dictionary elem taken from disk -> TERM: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " TERMID: " + le.getTermId() + " OFFSET: " + le.getOffsetDocId());
            return le;



        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void readDictionaryFromDisk(){

        System.out.println("Loading dictionary from disk...");

        long position = 0;

        MappedByteBuffer buffer; // get first term of the block
        try (
                FileChannel channel = new RandomAccessFile(DICTIONARY_FILE, "rw").getChannel()
        ) {
            long len = channel.size();

            while(position < len) {
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, DICT_ELEM_SIZE);
                position += DICT_ELEM_SIZE;

                DictionaryElem le = new DictionaryElem();           // create new DictionaryElem

                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to docno
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                if(charBuffer.toString().split("\0").length == 0)
                    continue;
                String term = charBuffer.toString().split("\0")[0];
                if(term.equals("00"))
                    System.out.println("TERM: " + term + " duplicated");

                le.setTerm(term); //split using end string character
                buffer.position(TERM_DIM); //skip docno
                le.setDf(buffer.getInt());
                le.setCf(buffer.getInt());
                le.setTermId(buffer.getInt());
                le.setOffsetTermFreq(buffer.getLong());
                le.setOffsetDocId(buffer.getLong());
                dictionary.getTermToTermStat().put(term, le);

                // print of the dictionary element fields taken from the disk
/*
                if(verbose && (position % 1000 == 0))
                    System.out.println("TERM: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " TERMID: " + le.getTermId() + " OFFSET: " + le.getOffsetDocId());
*/

            }

            for(DictionaryElem de :dictionary.getTermToTermStat().values())
                System.out.println("TERM: " + de.getTerm());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static PostingList readPostingListFromDisk(long offsetDocId, long offsetTermFreq, String term, int posting_size, FileChannel docidChannel, FileChannel termfreqChannel) {

        PostingList pl = new PostingList(term);
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
//                if(verbose)
//                    System.out.println(String.format("Posting list taken from disk -> TERM: " + term + " - TERMFREQ: " + termfreq + " - DOCID: " + docid));
                offsetDocId += INT_BYTES;
                offsetTermFreq += INT_BYTES;
            }
            if(IndexMerger.i % 1000 == 0) System.out.println("readPostingListFromDisk: " + pl);

            return pl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



    // method to free memory by deleting the information in document table, dictionary,and inverted index
    public static void freeMemory(){
        documentTable.clear();
        dictionary.getTermToTermStat().clear();
        invertedIndex.clear();
    }

}
