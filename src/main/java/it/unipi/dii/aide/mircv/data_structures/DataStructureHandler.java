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
import java.util.Collections;
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
    private static final Dictionary dictionary = new Dictionary();      // dictionary in memory
    private static final HashMap<String, ArrayList<Posting>> invertedIndex = new HashMap<>();   // hash table Term to related Posting list

    public static ArrayList<Long> dictionaryBlockOffsets = new ArrayList<>();       // Offsets of the dictionary blocks
    public static CollectionStatistics collection = new CollectionStatistics();     //

    /**
     * Implements the SPIMI algorithm for indexing large collections.
     */
    public static void SPIMIalgorithm() {

        long memoryAvailable = (long) (Runtime.getRuntime().maxMemory() * MEMORY_THRESHOLD);
        int docCounter = 1;         // counter for DocID
        int termCounter = 0;        // counter for TermID
        int totDocLen = 0;          // variable for the sum of the lengths of all documents

        try (
                BufferedReader buffer_collection = new BufferedReader(new InputStreamReader(new FileInputStream(COLLECTION_PATH), StandardCharsets.UTF_8));
        ) {
            String record;          // string to contain the document

            // scan all documents in the collection
            while ((record = buffer_collection.readLine()) != null) {

                // check for malformed line, no \t
                int separator = record.indexOf("\t");
                if (record.isBlank() || separator == -1) { // empty string or composed by whitespace characters or malformed
                    continue;
                }

                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text
                String docno = preprocessed.remove(0);      // get the DocNO of the current document

                // check if document is empty
                if (preprocessed.isEmpty() || (preprocessed.size() == 1 && preprocessed.get(0).equals("")))  {
                    continue;              // skip to next while iteration (next document)
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());   // create new Document element
                documentTable.put(docCounter, de);      // add current Document into Document Table in memory
                totDocLen += preprocessed.size();       // add current document length
                // scan all term in the current document
                for (String term : preprocessed) {
                    // control check if the length of the current term is greater than the maximum allowed
                    if(term.length() > TERM_DIM)
                        term = term.substring(0,TERM_DIM);                  // truncate term

                    // control check if the current term has already been found or is the first time
                    if (!dictionary.getTermToTermStat().containsKey(term))
                        termCounter++;                  // update TermID counter

                    assert !term.equals("");

                    DictionaryElem dictElem = dictionary.getOrCreateTerm(term,termCounter);     // Dictionary build

                    if(addTerm(term, docCounter, 0))
                        dictElem.addDf(1);
                    dictElem.addCf(1);

                    N_POSTINGS++;
                }
                docCounter++;       // update DocID counter

                if(Runtime.getRuntime().totalMemory() > memoryAvailable) {
                    System.out.println("********** Memory full **********");

                    storeIndexAndDictionaryIntoDisk();  //store index and dictionary to disk
                    storeDocumentTableIntoDisk(); // store document table one document at a time for each block

                    freeMemory();
                    System.gc();
                    System.out.println("********** Free memory **********");
                    N_POSTINGS = 0; // new partial index
                }
            }

            storeBlockOffsetsIntoDisk();

            collection.setnDocs(docCounter);        // set total number of Document in the collection
            collection.setTotDocLen(totDocLen);     // set the sum of the all document length in the collection

            storeCollectionStatsIntoDisk();         // store collection statistics into disk

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * Add the current term to the inverted index
     * @return false if the term has been already encountered in the current document,
     *         true if the term has been encountered for the first time in the current document or if the term was for the first time encountered
     * ***/
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
            invertedIndex.get(term).get(size - 1).addTermFreq(1);
            return false; // No need to increment df
        }
    }

    // -------- start: functions to store into disk --------
    // function to store the user's choices for the flags
    public static void storeFlagsIntoDisk() {
        System.out.println("Storing flags into disk...");

        try (
                RandomAccessFile raf = new RandomAccessFile(FLAGS_FILE, "rw");
                FileChannel channel = raf.getChannel()
        ) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) INT_BYTES * 3); //offset_size (size of dictionary offset) * number of blocks

            buffer.putInt(Flag.isSwsEnabled() ? 1 : 0);             // write stop words removal user's choice
            buffer.putInt(Flag.isCompressionEnabled() ? 1 : 0);     // write compression user's choice
            buffer.putInt(Flag.isScoringEnabled() ? 1 : 0);         // write scoring user's choice

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // function to store the collection statistics into disk
    public static void storeCollectionStatsIntoDisk() {
        System.out.println("Storing collection statistics into disk...");

        try (
                RandomAccessFile docStats = new RandomAccessFile(STATS_FILE, "rw");
                FileChannel channel = docStats.getChannel()
        ) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) (INT_BYTES * 1 + DOUBLE_BYTES * 1)); // integer size * number of int to store (1) + double size * number of double to store (1)

            buffer.putInt(collection.getnDocs());           // write total number of document in collection
            buffer.putDouble(collection.getTotDocLen());    // write sum of the all document length in the collection

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // function to store the whole document table into disk
    private static void storeDocumentTableIntoDisk() {

        try (RandomAccessFile raf = new RandomAccessFile(DOCTABLE_FILE, "rw");
             FileChannel channel = raf.getChannel()) {

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), (long) DOCELEM_SIZE *documentTable.size());

            // Buffer not created
            if(buffer == null)
                return;
            // scan all document elements of the Document Table
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

    // function to store offset of the blocks into disk
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

            // scan all block and for each one write offset into disk
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

    /**
     * function to store one dictionary elem into disk
     *
     * @param dictElem  Element of the dictionary that contains the value to write into disk
     * @param channel   indicate the file where to write
     */
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
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));      // write term
            buffer.putInt(dictElem.getDf());                            // write Df
            buffer.putInt(dictElem.getCf());                            // write Cf
            buffer.putInt(dictElem.getTermId());                        // write TermID
            buffer.putLong(dictElem.getOffsetTermFreq());               // write offset Tf
            buffer.putLong(dictElem.getOffsetDocId());                  // write offset DID
            PARTIAL_DICTIONARY_OFFSET += DICT_ELEM_SIZE;        // update offset

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
                    System.out.println(ANSI_CYAN + "term: 0000 " + dictionary.getTermToTermStat().get("0000") +  " block " + (dictionaryBlockOffsets.size()-1) + " size: " + posList.size() +ANSI_RESET);

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
                    if(term.equals("0000"))
                        System.out.println(ANSI_YELLOW + "docId " + posting.getDocId() +  " termfreq " + posting.getTermFreq() + ANSI_RESET);
                    INDEX_OFFSET += INT_BYTES;
                }

                // store dictionary entry to disk
                storeDictionaryElemIntoDisk(dictElem, dictChannel);
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
    // function to read the user's choices for the flags
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
            int isSwsEnabled = flagsBuffer.getInt();            // read stop words removal user's choice
            int isCompressionEnabled = flagsBuffer.getInt();    // read compression user's choice
            int isScoringEnabled = flagsBuffer.getInt();        // scoring user's choice

            // Set flag values with values read
            Flag.setSws(isSwsEnabled == 1);
            Flag.setCompression(isCompressionEnabled == 1);
            Flag.setScoring(isScoringEnabled == 1);

            System.out.println("*** Flags read -> sws: " + isSwsEnabled + ", compression: " + isCompressionEnabled + ", scoring: " + isScoringEnabled);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // function to read the collection statistics from disk
    public static void readCollectionStatsFromDisk() {

        System.out.println("Loading collection statistics from disk...");

        try (RandomAccessFile statsRAF = new RandomAccessFile(new File(STATS_FILE), "rw")) {
            ByteBuffer statsBuffer = ByteBuffer.allocate(INT_BYTES * 1 + DOUBLE_BYTES * 1);   // bytes to read from disk
            statsRAF.getChannel().position(0);

            statsRAF.getChannel().read(statsBuffer);            // Read flag values from file
            statsBuffer.rewind();                               // Move to the beginning of file for reading

            // Get collection statistic values from buffer
            int nDocs = statsBuffer.getInt();               // read number of documents in the collection
            double totDocLen = statsBuffer.getDouble();     // read sum of the all document length in the collection

            // Set collection statistics values with values read
            collection.setnDocs(nDocs);
            collection.setTotDocLen(totDocLen);

            System.out.println("*** Collection statistics read -> nDocs: " + nDocs + ", totDocLen: " + totDocLen);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    /**
     * function to read one Document Element from disk
     *
     * @param start     offset of the document reading from document file
     * @param channel   indicate the file from which to read
     * @return a DocumentElement with the value read from disk
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
        buffer.position(DOCNO_DIM);             //skip docno
        de.setDocid(buffer.getInt());
        de.setDoclength(buffer.getInt());

        // print of the document element fields taken from the disk
        if((start % printInterval == 0) && verbose)
            System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());

        return de;
    }

    // function to read all document table from disk and put it in memory (HashMap documentTable)
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

    // function to read offset of the block from disk
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
                if (verbose)
                    System.out.println("OFFSET BLOCK " + i + ": " + dictionaryBlockOffsets.get(i));
            }

            System.out.println(dictionaryBlockOffsets.size() + " blocks loaded");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * function to read one Dictionary Element from disk
     *
     * @param start     offset of the document reading from document file
     * @param channel   indicate the file from which to read
     * @return a DictionaryElem with the value read from disk
     */
    public static DictionaryElem readDictionaryElemFromDisk(long start, FileChannel channel){

        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start,  DICT_ELEM_SIZE); // get first term of the block
            DictionaryElem le = new DictionaryElem();           // create new DictionaryElem
            CharBuffer.allocate(TERM_DIM);              //allocate a charbuffer of the dimension reserved to docno
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            if(!(charBuffer.toString().split("\0").length == 0))
                le.setTerm(charBuffer.toString().split("\0")[0]);   // read term

            buffer.position(TERM_DIM);                  // skip over term position
            le.setDf(buffer.getInt());                  // read Df
            le.setCf(buffer.getInt());                  // read Cf
            le.setTermId(buffer.getInt());              // read TermID
            le.setOffsetTermFreq(buffer.getLong());     // read offset Tf
            le.setOffsetDocId(buffer.getLong());        // read offset DID

//            // print of the dictionary element fields taken from the disk
//            if (verbose && (printInterval % start == 0))
//                System.out.println("Dictionary elem taken from disk -> TERM: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " TERMID: " + le.getTermId() + " OFFSET: " + le.getOffsetDocId());
            return le;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // function to read whole Dictionary from disk
    public static void readDictionaryFromDisk(){
        System.out.println("Loading dictionary from disk...");

        long position = 0;      // indicate the position where read at each iteration

        MappedByteBuffer buffer;    // get first term of the block
        try (
                FileChannel channel = new RandomAccessFile(DICTIONARY_FILE, "rw").getChannel()
        ) {
            long len = channel.size();          // size of the dictionary saved into disk

            // scan all Dictionary Element saved into disk
            while(position < len) {
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, DICT_ELEM_SIZE);// read one DictionaryElem
                position += DICT_ELEM_SIZE;                     // update read position

                DictionaryElem le = new DictionaryElem();       // create new DictionaryElem

                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to docno
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                // control check of the term size
                if(charBuffer.toString().split("\0").length == 0)
                    continue;

                String term = charBuffer.toString().split("\0")[0];     // read term

                if(term.equals("epstein"))
                    System.out.println("TERM: " + term + " duplicated");

                le.setTerm(term);                           //split using end string character
                buffer.position(TERM_DIM);                  //skip docno
                le.setDf(buffer.getInt());                  // read and set Df
                le.setCf(buffer.getInt());                  // read and set Cf
                le.setTermId(buffer.getInt());              // read and set TermId
                le.setOffsetTermFreq(buffer.getLong());     // read and set offset Tf
                le.setOffsetDocId(buffer.getLong());        // read and set offset DID

                dictionary.getTermToTermStat().put(term, le);   // add DictionaryElem into memory
            }

            System.out.println("vocabulary size: " + dictionary.getTermToTermStat().size());
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
                if(verbose)
                    System.out.println("Posting list taken from disk -> " + pl);
            }
            return pl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * function to obtain the complete posting list (stored into disk) related the term passed by parameter
     *
     * @param term
     * @return
     */
    public static ArrayList<Posting> getPostingListFromTerm(String term)
    {
        ArrayList<Posting> postingList = new ArrayList<>();     // contain the posting list of term
        DictionaryElem de;

        try (
                // open complete files to read the postingList
                RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");
                // FileChannel
                FileChannel docIdChannel = docidFile.getChannel();
                FileChannel termFreqChannel = termfreqFile.getChannel();
        ) {
            // check if the term is the term is present in the dictionary hashmap
            if (dictionary.getTermToTermStat().containsKey(term))
            {
                de = dictionary.getTermToTermStat().get(term);      // take DictionaryElem related term
                // take the postingList of term
                postingList = readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf(),docIdChannel,termFreqChannel);
                //if(verbose)
                printPostingList(postingList,term);          // print posting list
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return postingList;
    }
    // -------- end: functions to read from disk --------

    // method to free memory by deleting the information in document table, dictionary,and inverted index
    public static void freeMemory(){
        documentTable.clear();
        dictionary.getTermToTermStat().clear();
        invertedIndex.clear();
    }

    // method to return the length of the posting list related to the term passed as parameter
    public static int postingListLengthFromTerm (String term)
    {
        if (dictionary.getTermToTermStat().containsKey(term))
            return dictionary.getTermToTermStat().get(term).getDf();
        else
            return 0;
    }

    // -------- start: function to manage the file in disk --------

    /**
     * Function that check if there are all .txt files in "/resources/merged" folder
     * The file that controls are: dictionary.txt, docId.txt, documentTable.txt, termFreq.txt
     *
     * @return  true -> there are all merged files into disk
     *          false -> there aren't all merged files into disk
     */
    public static boolean areThereAllMergedFiles()
    {
        // define all file
        File docTable = new File(DOCTABLE_FILE);        // documentTable.txt
        File dict = new File(DICTIONARY_FILE);          // dictionary.txt"
        File docDID = new File(DOCID_FILE);             // docId.txt
        File docTF = new File(TERMFREQ_FILE);           // termFreq.txt

        return docTable.exists() && dict.exists() && docDID.exists() && docTF.exists();
    }

    /**
     * Function that check if there is the 'flags.txt' file in "/resources" folder
     *
     * @return  true -> there is
     *          false -> there isn't
     */
    public static boolean isThereFlagsFile()
    {
        // define file
        File docFlags = new File(FLAGS_FILE);        // flags.txt

        return docFlags.exists();
    }

    /**
     * Function that check if there is the 'collectionStatistics.txt' file in "/resources" folder
     *
     * @return  true -> there is
     *          false -> there isn't
     */
    public static boolean isThereStatsFile()
    {
        // define file
        File docStats = new File(STATS_FILE);        // collectionStatistics.txt

        return docStats.exists();
    }

    // -------- end: function to manage the file in disk --------

    // -------- start: function to check if the structures in memory are set --------

    // function that return if the dictionary in memory is set or not
    public static boolean dictionaryIsSet()
    {
        return dictionary.getTermToTermStat().size() != 0;  // the hash map in dictionary is empty, the dictionary isn't set
    }

    // -------- end: function to check if the structures in memory are set --------

    // -------- start: utility function and function useful for testing

    // function to show in console a posting list passed by parameter
    public static void printPostingList(ArrayList<Posting> pl,String term)
    {
        int position = 0;       // var that indicate the position of current posting in the posting list

        System.out.println("printPostingList of " + term + ": ");
        // iterate through all posting in the posting list
        for (Posting p : pl)
        {
            System.out.println("Posting #:" + position + " DocID: " + p.getDocId() + " Tf: " + p.getTermFreq());
            position++;         // update iterator
        }
    }
    // -------- start: utility function and function useful for testing

}
