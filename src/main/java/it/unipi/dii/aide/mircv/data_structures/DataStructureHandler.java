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

/**
 * This class handles the storage and retrieval of data structures used for document indexing.
 */
public class DataStructureHandler {

    // Constants for file paths
    private static final String DOCUMENT_FILE = "src/main/resources/document.txt"; // file in which is stored the document table
    private static final String VOCABULARY_FILE = "src/main/resources/vocabulary.txt"; // file in which is stored the vocabulary
    private static final String DOCID_FILE = "src/main/resources/docid.txt";
    private static final String TERMFREQ_FILE = "src/main/resources/termfreq.txt";

    private static final int DOCNO_DIM = 10;        // Length of docno (in bytes)
    private static final int TERM_DIM = 32;          // Length of a term (in bytes)
    private static int N_POSTINGS = 0;               // Number of partial postings to save in the file
    private static long DICTIONARY_OFFSET = 0;       // Offset of the terms in the dictionary

    // Data structures initialization
    private static DocumentTable dt = new DocumentTable();
    private static final Dictionary dictionary = new Dictionary();
    private static final InvertedIndex invertedIndex = new InvertedIndex();

    private static ArrayList<Long> indexBlocks;         // Offsets of the InvertedIndex blocks
    private static ArrayList<Long> dictionaryBlocks;    // Offsets of the dictionary blocks

    public static RandomAccessFile Flags_raf;

    /**
     * Initializes data structures and fills them from the input collection.
     *
     * @throws IOException if there is an error reading the collection.
     */
    public static void initializeDataStructures() throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.COLLECTION_PATH), StandardCharsets.UTF_8))) {

            System.out.println("\n*** Data structure build ***\n");

            int docCounter = 1;         // Counter to indicate the DocID of the current document
            int termCounter = 1;        // Counter to indicate the TermID of the current term
            String record;

            // Scroll through the dataset documents
            while ((record = br.readLine()) != null) {
                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text

                String docno = preprocessed.remove(0);      // get the DocNO of the current document

                if (preprocessed.isEmpty()) {
                    continue;              // Empty documents, skip to next while iteration
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());
                storeDocumentTableElementIntoDisk(de); // Store document table one document at a time
                docCounter++; // Update DocID counter

                if(docCounter == 10000)
                    return;

                // scroll through the term of the document
                for (String term : preprocessed) {
                    // Dictionary build
                    if(term.length() > TERM_DIM)
                        term = term.substring(0, TERM_DIM);

                    DictionaryElem dictElem = dictionary.getOrCreateTerm(term, termCounter);
                    termCounter++;         // update TermID counter

                    // Build inverted index
                    // "addTerm" add posting in inverted index and return true if term is in a new doc -> update df
                    if (invertedIndex.addTerm(term, docCounter, 0)) {
                        dictElem.incDf();                // increment Document Frequency of the term in the dictionary
                    }
                    N_POSTINGS++;

                    // test print for dictionary
                    /*
                    if (termCounter < 10) {
                        HashMap<String, DictionaryElem> dictElem = dictionary.getTermToTermStat();
                        System.out.println("********** Dictionary **********");
                        System.out.println("Term: " + term);
                        System.out.println("TermId: " + termCounter + " - TermId elem:" + dictElem.get(term).getTermId());
                        System.out.println("Df: " + dictElem.get(term).getDf());
                        System.out.println("Cf: " + dictElem.get(term).getCf());
                        System.out.println("Dictionary size: " + dictElem.size());
                    }
                    */
                }
              /* // test print for documentElement
                if (docCounter == 53) {
                    HashMap<Integer, DocumentElement> doctable = dt.getDocIdToDocElem();
                    System.out.println("********** Document Table **********");
                    System.out.println("Docid: " + docCounter);
                    System.out.println("DocTable size: " + doctable.size());
                    System.out.println("Docno: " + doctable.get(docCounter - 1).getDocno());
                    System.out.println("Length: " + doctable.get(docCounter - 1).getDoclength());
                }
                */
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implements the SPIMI algorithm for indexing large collections.
     */
    public static void SPIMIalgorithm() {

        long memoryAvailable = Runtime.getRuntime().maxMemory() * 70 / 100;
        int docCounter = 0;
        int termCounter = 0;
        int printInterval = 500; // Print memory usage every 500 documents

        indexBlocks = new ArrayList<>();
        dictionaryBlocks = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.COLLECTION_PATH), StandardCharsets.UTF_8))) {

            String record;

            while ((record = br.readLine()) != null) {
                if(record.isBlank()) // empty string or composed by whitespace characters
                    continue;

                if (docCounter % printInterval == 0)
                    System.out.println("MEMORY AVAILABLE : " + memoryAvailable);

                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text

                String docno = preprocessed.remove(0);      // get the DocNO of the current document
                if (preprocessed.isEmpty()) {
                    continue;              // Empty documents, skip to next while iteration
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());
                DataStructureHandler.storeDocumentTableElementIntoDisk(de); // store document table one document at a time
                docCounter++;              // update DocID counter

                for (String term : preprocessed) {

                    // Dictionary build
                    if(term.length() > 32)
                        term = term.substring(0,32);

                    DictionaryElem dictElem = dictionary.getOrCreateTerm(term, termCounter);
                    termCounter++;         // update TermID counter

                    // Build inverted index
                    // "addTerm" add posting in inverted index and return true if term is in a new doc -> update df
                    if (invertedIndex.addTerm(term, docCounter, 0)) {
                        dictElem.incDf();                // increment Document Frequency of the term in the dictionary
                    }
                    N_POSTINGS++;
                    //System.out.println("*** NPOSTINGS: " + npostings + "***");
                }

                if (docCounter % printInterval == 0)
                    System.out.println("TOT MEMORY: " + Runtime.getRuntime().totalMemory() + " - MEM AVAILABLE: " + memoryAvailable);

                if(Runtime.getRuntime().totalMemory() > memoryAvailable) {
                    System.out.println("********** Memory full **********");
                    //store index and dictionary to disk
                    storeIndexAndVocabularyIntoDisk(invertedIndex);
                    //free memory
                    freeMemory();
                    System.gc();
                    System.out.println("*********** Free memory **********");
                    N_POSTINGS = 0; // new partial index

                }

                if(docCounter == 10000) {
                    System.out.println(" II: " + invertedIndex.getInvertedIndex().get("the"));
                    storeIndexAndVocabularyIntoDisk(invertedIndex);
                    freeMemory();
                    return;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // -- start of store and get functions --

    public static void storeCollectionIntoDisk(){

    }

    public static void storeFlagsIntoDisk() throws IOException {

        ByteBuffer FlagsBuffer = ByteBuffer.allocate(12);
        Flags_raf.getChannel().position(0);

        FlagsBuffer.putInt(Flag.isSwsEnabled() ? 1 : 0);
        FlagsBuffer.putInt(Flag.isScoringEnabled() ? 1 : 0);
        FlagsBuffer.putInt(Flag.isCompressionEnabled() ? 1 : 0);

        FlagsBuffer = ByteBuffer.wrap(FlagsBuffer.array());

        while (FlagsBuffer.hasRemaining())
            Flags_raf.getChannel().write(FlagsBuffer);
    }

    // Read document table elements from file
    public static void storeDocumentTableElementIntoDisk(DocumentElement de){

        //System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());

        try (RandomAccessFile raf = new RandomAccessFile(DOCUMENT_FILE, "rw");
             FileChannel channel = raf.getChannel()) {

            int docsize = 4 + DOCNO_DIM + 4; // Size in bytes of docid, docno, and doclength
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), docsize);

            // Buffer not created
            if(buffer == null)
                return;

            //allocate bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(DOCNO_DIM);

            //put every char into charbuffer
            for(int i = 0; i < de.getDocno().length(); i++)
                charBuffer.put(i, de.getDocno().charAt(i));

            // write docno, docid and doclength into document file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
            buffer.putInt(de.getDocid());
            buffer.putInt(de.getDoclength());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void storeDictionaryIntoDisk(DictionaryElem dictElem){
        try (RandomAccessFile raf = new RandomAccessFile(VOCABULARY_FILE, "rw");
             FileChannel channel = raf.getChannel()) {
            int vocsize = TERM_DIM + 4 + 4 + 4 + 8 + 8; // Size in bytes of term, df, cf, termId, offset
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

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void storeIndexAndVocabularyIntoDisk(InvertedIndex ii){

        final int INT_SIZE = 4;

        // Sort the inverted index lexicographically
        invertedIndex.sort();
        System.out.println("SORTED");

        try (FileChannel docidChannel = new RandomAccessFile(DOCID_FILE, "rw").getChannel();
             FileChannel termfreqChannel = new RandomAccessFile(TERMFREQ_FILE, "rw").getChannel()) {

            indexBlocks.add(docidChannel.size() + 1); //update the offset of the block for the docid file

            // Create buffers for docid and termfreq
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) N_POSTINGS * INT_SIZE); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, 0, (long) N_POSTINGS * INT_SIZE); //from 0 to number of postings * int dimension

            // iterate through all the posting list of all the terms
            for (PostingList posList : ii.getInvertedIndex().values()) {
                System.out.println("P: " + posList.getTerm());          // print current term of the InvertedIndex

                DictionaryElem dictElem = dictionary.getTermStat(posList.getTerm());
                dictElem.setOffsetTermFreq(DICTIONARY_OFFSET);
                dictElem.setOffsetDocId(DICTIONARY_OFFSET);

                //iterate through postings in the posting list
                for (Posting posting : posList.getPostings()) {

                    // Buffer not created
                    if(bufferdocid == null || buffertermfreq == null)
                        return;

                    // Write DocID and TermFreq to buffers
                    //System.out.println("OF+DOCID: " + posting.getDocId() + " TERMFREQ: " + posting.getTermFreq());
                    bufferdocid.putInt(posting.getDocId());         // write DocID
                    buffertermfreq.putInt(posting.getTermFreq());   // write TermFrequency
                    DICTIONARY_OFFSET += INT_SIZE;
                }
                // store dictionary entry to disk
                storeDictionaryIntoDisk(dictElem);
            }
            dictionaryBlocks.add(termfreqChannel.size());// update of the offset of the block for the dictionary file
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static void getCollectionFromDisk() {

    }
    public static void getFlagsFromDisk() {

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
            //System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());
            return de;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void getIndexFromDisk() {
        int indexsize = 4; //docId and termFreq are int, 4Byte each
        int read = 0;

        try (FileChannel docidChannel = new RandomAccessFile(DOCID_FILE, "rw").getChannel(); FileChannel termfreqChannel = new RandomAccessFile(TERMFREQ_FILE, "rw").getChannel()) {
            // iterate through each term in dictionary
            for (String term : dictionary.getTermToTermStat().keySet()) {
                read = 0;
                long offsetDocid = dictionary.getTermToTermStat().get(term).getOffsetDocId();
                long offsetTermFreq = dictionary.getTermToTermStat().get(term).getOffsetTermFreq();
                int len = dictionary.getTermToTermStat().get(term).getDf()*4; //number of postings for the term
//                System.out.println("TERM: " + term + " OFFSET: " + offsetDocid + " END: " + (offsetDocid + len));
                System.out.println(String.format("TERM: %20s - OFFSET: %10s - DOCID: %10s", term, offsetDocid, (offsetDocid + len)));


                MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_WRITE, offsetDocid, offsetDocid + len); //put into buffer from offset to offset plus the number of postings * 4 (int dimension)
                MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, offsetTermFreq, offsetTermFreq + len);

                while (read < len) {
                    int docid = docidBuffer.getInt();
                    int termfreq = termfreqBuffer.getInt();
//                    System.out.println("TERM: " + term + " TERMFREQ: " + termfreq + " DOCID: " + docid);
                    System.out.println(String.format("TERM: %20s - TERMFREQ: %8s - DOCID: %10s", term, termfreq, docid));

                    invertedIndex.addTerm(term, docid, termfreq);
                    read += 4;
                    docidBuffer.position(read);
                    termfreqBuffer.position(read);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // read each block
    public static void getIndexBlockFromDisk() {
        int indexsize = 4; //docId and termFreq are int, 4Byte each
        int read = 0;

        try (FileChannel docidChannel = new RandomAccessFile(DOCID_FILE, "rw").getChannel(); FileChannel termfreqChannel = new RandomAccessFile(TERMFREQ_FILE, "rw").getChannel()){
            for (String term : dictionary.getTermToTermStat().keySet()) {
                long offsetDocid = dictionary.getTermToTermStat().get(term).getOffsetDocId();
                long offsetTermFreq = dictionary.getTermToTermStat().get(term).getOffsetTermFreq();
                int len = dictionary.getTermToTermStat().get(term).getDf();

                MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_WRITE, offsetDocid, offsetDocid + dictionary.getTermToTermStat().get(term).getDf());
                MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, offsetTermFreq, offsetTermFreq + dictionary.getTermToTermStat().get(term).getDf());

                while (read < len) {
                    System.out.println("TERM: " + term + " TERMFREQ: " + termfreqBuffer.getInt() + " DOCID: " + docidBuffer.getInt());
                    invertedIndex.addTerm(term, docidBuffer.getInt(), termfreqBuffer.getInt());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // retrieve all the dictionary from the disk
    public static void getDictionaryFromDisk() {
        int vocsize = TERM_DIM + 4 + 4 + 4 + 8 + 8; // Size in bytes of term, df, cf, termId, offsetTermFreq, offsetDocId

        // read dictionary from disk
        try (FileChannel channel = new RandomAccessFile(VOCABULARY_FILE, "rw").getChannel()) {
            for (int i = 0; i < channel.size(); i += vocsize) { //iterate through all the vocabulary file
                DictionaryElem le = new DictionaryElem();         // create new DictionaryElem

                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, i, vocsize);

                // Buffer not created
                if (buffer == null)
                    continue;

                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reservated to term
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

                le.setTerm(charBuffer.toString().split("\0")[0]); //split using end string character
                buffer.position(TERM_DIM); //skip term
                le.setCf(buffer.getInt());
                le.setDf(buffer.getInt());
                le.setTermId(buffer.getInt());
                le.setOffsetTermFreq(buffer.getLong());
                le.setOffsetDocId(buffer.getLong());
                dictionary.getTermToTermStat().put(le.getTerm(), le);  // add term and DictionaryElem to dictionary
                System.out.println("Term: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " OFFSET: " + le.getOffsetDocId());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void freeMemory(){
        dt.getDocIdToDocElem().clear();
        dictionary.getTermToTermStat().clear();
        invertedIndex.getInvertedIndex().clear();
    }

    // -- start of set and get functions --

    public static void setDt(DocumentTable dt) {
        DataStructureHandler.dt = dt;
    }

    public static DocumentTable getDt() {
        return dt;
    }

}
