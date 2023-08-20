package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.Main;
import it.unipi.dii.aide.mircv.TextProcessor;
import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;


public class DataStructureHandler {

    private static final String documentFile = "src/main/resources/document.txt"; // file in which is stored the document table
    private static final String vocabularyFile = "src/main/resources/vocabulary.txt"; // file in which is stored the vocabulary
    private static final String docidFile = "src/main/resources/docid.txt";
    private static final String termfreqFile = "src/main/resources/termfreq.txt";

    private static final int docnodim  = 20; //docno of 20 bytes
    private static final int termdim = 64; //term of 64 bytes

    private static int npostings = 0; //number of partial postings to save in the file
    private static final long offsetLexicon = 0; //offset of the terms in the lexicon

    // data structures initialization
    private static DocumentTable dt = new DocumentTable();
    private static final Lexicon lexicon = new Lexicon();
    private static final InvertedIndex invertedIndex = new InvertedIndex();


    /**
     * Function to create and fill the lexicon, document table and inverted index
     * @throws IOException
     */
    public static void initializeDataStructures() throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.collection_path), StandardCharsets.UTF_8))) {

            System.out.println("\n* Initializing data structures...");     // print for the User Interface

            int docCounter = 1;         // Counter to indicate the DocID of the current document
            int termCounter = 1;        // Counter to indicate the TermID of the current term

            String record;

            // scroll through the dataset documents
            while ((record = br.readLine()) != null) {

                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text
                String docno = preprocessed.remove(0);      // get the DocNO of the current document

                if (preprocessed.isEmpty()) {
                    continue;              // Empty documents, skip to next while iteration
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());
                storeDocumentTableElementIntoDisk(de); // store document table one document at a time
                docCounter++;              // update DocID counter

                if(docCounter == 10000)
                    return;

                // scroll through the term of the document
                for (String term : preprocessed) {
                    // Lexicon build
                    term = term.substring(0, Math.min(term.length(), termdim)); // Ensure term length doesn't exceed termdim
                    LexiconElem lexElem = lexicon.getOrCreateTerm(term, termCounter);
                    termCounter++;         // update TermID counter

                    // Build inverted index
                    // "addTerm" add posting in inverted index and return true if term is in a new doc -> update df
                    if (invertedIndex.addTerm(term, docCounter, 0)) {
                        lexElem.incDf();                // increment Document Frequency of the term in the lexicon
                    }
                    npostings++;

                    // test print for lexicon
                    /*
                    if (termCounter < 10) {
                        HashMap<String, LexiconElem> lex = lexicon.getTermToTermStat();
                        System.out.println("********** Lexicon **********");
                        System.out.println("Term: " + term);
                        System.out.println("TermId: " + termCounter + " - TermId elem:" + lex.get(term).getTermId());
                        System.out.println("Df: " + lex.get(term).getDf());
                        System.out.println("Cf: " + lex.get(term).getCf());
                        System.out.println("Lexicon size: " + lex.size());
                    }
                    */
                }

                if (Runtime.getRuntime().totalMemory() > Runtime.getRuntime().maxMemory() * 70 / 100) {
                    storeIndexAndVocabularyIntoDisk(invertedIndex);
                    freeMemory();
                    System.gc();
                    npostings = 0;
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


        }
        System.out.println("V Data structures initialized");
    }

    public static void getCollectionFromDisk() {

    }
    public static void getFlagsFromDisk() {

    }

   /**
    @param start offset of the document reading from document file
    **/
    public static DocumentElement getDocumentIndexFromDisk(int start) {

        System.out.println("\n* Retrieving document index from disk...");

        DocumentElement de = new DocumentElement();
        try (FileChannel channel = new RandomAccessFile(documentFile, "rw").getChannel()) {
            int docsize = 4 + docnodim + 4; // Size in bytes of docid, docno, and doclength
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, start, docsize);

            // Buffer not created
            if(buffer == null)
                return null;

            CharBuffer.allocate(docnodim); //allocate a charbuffer of the dimension reservated to docno
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            if(charBuffer.toString().split("\0").length == 0)
                return null;

            de.setDocno(charBuffer.toString().split("\0")[0]); //split using end string character
            buffer.position(docnodim); //skip docno
            de.setDoclength(buffer.getInt());
            de.setDocid(buffer.getInt());
            //System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());
            return de;
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("V Document index retrieved from disk");
        return null;
    }
/*    public static void getIndexFromDisk(){
        int indexsize = 4; //docId and termFreq

        try (FileChannel docidChannel = new RandomAccessFile(docidFile, "rw").getChannel(); FileChannel termfreqChannel = new RandomAccessFile(termfreqFile, "rw").getChannel()) {

            for(String term : lexicon.getTermToTermStat().keySet()){
                long offset = lexicon.getTermToTermStat().get(term).getOffset();
                for(int i = 0; i < lexicon.getTermToTermStat().get(term).getDf(); i++) {
                    MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_WRITE, offset, offset + indexsize);
                    MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, offset, offset + indexsize);

                    int docid = docidBuffer.getInt();
                    int termfreq = termfreqBuffer.getInt();
                    System.out.println("TERM: " + term + " TERMFREQ: " + termfreq + " DOCID: " + docid);
                    invertedIndex.addTerm(term, docid, termfreq);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }*/
    // retrieve all the dictionary from the disk
    public static void getDictionaryFromDisk() {
        System.out.println("\n* Retrieving dictionary from disk...\n");
        int vocsize = termdim + 4 + 4 + 4 + 8; // Size in bytes of term, df, cf, termId, offset

        try (FileChannel channel = new RandomAccessFile(vocabularyFile, "rw").getChannel()) {
            for (int i = 0; i < channel.size(); i += vocsize) { //iterate through all the vocabulary file
                LexiconElem le = new LexiconElem();

                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, i, vocsize);

                // Buffer not created
                if (buffer == null)
                    continue;

                CharBuffer.allocate(termdim); //allocate a charbuffer of the dimension reservated to term
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

                if (charBuffer.toString().split("\0").length == 0)
                    continue;

                le.setTerm(charBuffer.toString().split("\0")[0]); //split using end string character
                buffer.position(termdim); //skip term
                le.setCf(buffer.getInt());
                le.setDf(buffer.getInt());
                le.setTermId(buffer.getInt());
                le.setOffset(buffer.getLong());

                lexicon.getTermToTermStat().put(le.getTerm(), le);

                System.out.println("Term: " + le.getTerm() + " CF: " + le.getCf() + " DF: " + le.getDf() + " OFFSET: " + le.getOffset());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nV dictionary retrieved from disk");
    }

    public static void storeCollectionIntoDisk(){

    }

    public static void storeFlagsIntoDisk(){

    }

    // Read document table elements from file
    public static void storeDocumentTableElementIntoDisk(DocumentElement de) {

        try (FileChannel channel = new RandomAccessFile(documentFile, "rw").getChannel()) {
            int docCounter = 0;
            int docsize = 4 + docnodim + 4; // Size in bytes of docid, docno, and doclength
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), docsize);

            // Buffer not created
            if (buffer == null)
                return;

            // Allocate 20 bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(docnodim);
            // Put each character into charbuffer
            charBuffer.put(de.getDocno(), 0, Math.min(de.getDocno().length(), docnodim));
            // Write docno, docid, and doclength into the document file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
            buffer.putInt(de.getDocid());
            buffer.putInt(de.getDoclength());

            // Increment the document counter
            docCounter++;

            // Print the information every 500 documents
            if (docCounter % 500 == 0) {
                System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void storeDictionaryIntoDisk(LexiconElem le) {
        System.out.println("\n* Storing dictionary into disk...");

        try (FileChannel channel = new RandomAccessFile(vocabularyFile, "rw").getChannel()) {
            int termLength = Math.min(le.getTerm().length(), termdim);

            // Calculate the total size of the record (term, df, cf, termId, offset)
            int recordSize = termLength + Integer.BYTES * 3 + Long.BYTES;

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), recordSize);

            // Check if the buffer was created successfully
            if (buffer != null) {
                // Write the term into the buffer
                CharBuffer charBuffer = CharBuffer.allocate(termdim);
                charBuffer.put(le.getTerm(), 0, termLength);
                buffer.put(StandardCharsets.UTF_8.encode(charBuffer));

                // Write df, cf, termId, and offsetLexicon into the buffer
                buffer.putInt(le.getDf());
                buffer.putInt(le.getCf());
                buffer.putInt(le.getTermId());
                buffer.putLong(offsetLexicon);

                // Make sure to write all data in the buffer before proceeding
                buffer.force();

                // Print the data just written to the buffer
                System.out.println("Stored in dictionary file:");
                System.out.println("Term: " + le.getTerm() +
                        " DF: " + le.getDf() +
                        " CF: " + le.getCf() +
                        " TermId: " + le.getTermId() +
                        " OffsetLexicon: " + offsetLexicon);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("\nV dictionary stored into disk...");
    }



    public static void storeIndexAndVocabularyIntoDisk(InvertedIndex ii){
        System.out.println("\n* Store index and vocabulary into disk...");
        //sort in lexicographic order the terms of the index
        invertedIndex.sort();
        System.out.println("SORTED");

        try (FileChannel docidchannel = new RandomAccessFile(docidFile, "rw").getChannel()) {

            MappedByteBuffer bufferdocid = docidchannel.map(FileChannel.MapMode.READ_WRITE, 0, npostings*4);
            MappedByteBuffer buffertermfreq = docidchannel.map(FileChannel.MapMode.READ_WRITE, 0, npostings*4);

            for (PostingList posList : ii.getInvertedIndex().values()) {
                System.out.println("P: " + posList.getTerm());
                for (Posting posting : posList.getPostings()) {
                    //store index into disk

                    // Buffer not created
                    if(bufferdocid == null || buffertermfreq == null)
                        return;

                    //write docId and termFreq
                    //System.out.println("OF+DOCID: " + posting.getDocId() + " TERMFREQ: " + posting.getTermFreq());
                    bufferdocid.putInt(posting.getDocId());
                    buffertermfreq.putInt(posting.getTermFreq());
                    //offsetLexicon += 4;
                }
                // store dictionary into the disk
                storeDictionaryIntoDisk(lexicon.getTermStat(posList.getTerm()));
            }
        } catch (IOException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }
        System.out.println("V index and vocabulary stored into disk");
    }


    public static void SPIMIalgorithm() {

        long memoryAvailable = Runtime.getRuntime().maxMemory() * 70 / 100;
        int docCounter = 0;
        int termCounter = 0;
        int printInterval = 500; // Print memory usage every 500 documents

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.collection_path), StandardCharsets.UTF_8))) {

            String record;

            while ((record = br.readLine()) != null) {
                if(record.isBlank()) // empty string or composed by whitespace characters
                    continue;

                ArrayList<String> preprocessed = TextProcessor.preprocessText(record); // Preprocessing of document text

                String docno = preprocessed.remove(0);      // get the DocNO of the current document
                if (preprocessed.isEmpty()) {
                    continue;              // Empty documents, skip to next while iteration
                }

                DocumentElement de = new DocumentElement(docno, docCounter, preprocessed.size());
                DataStructureHandler.storeDocumentTableElementIntoDisk(de); // store document table one document at a time
                docCounter++;              // update DocID counter

                for (String term : preprocessed) {
                    // Lexicon build
                    LexiconElem lexElem = lexicon.getOrCreateTerm(term, termCounter);
                    termCounter++;         // update TermID counter


                    // Build inverted index
                    // "addTerm" add posting in inverted index and return true if term is in a new doc -> update df
                    if (invertedIndex.addTerm(term, docCounter, 0)) {
                        lexElem.incDf();                // increment Document Frequency of the term in the lexicon
                    }
                    npostings++;
                    //System.out.println("*** NPOSTINGS: " + npostings + "***");

                }

                // Check memory usage and print memory usage every N documents
                if (docCounter % printInterval == 0) {
                    long totalMemory = Runtime.getRuntime().totalMemory();
                    System.out.println("TOT MEMORY: " + totalMemory + " MEM AVAILABLE: " + memoryAvailable);
                }
                if(Runtime.getRuntime().totalMemory() > memoryAvailable) {
                    System.out.println("********** Memory full **********");

                    //store index and lexicon to disk
                    storeIndexAndVocabularyIntoDisk(invertedIndex);
                    //free memory
                    freeMemory();
                    System.gc();
                    System.out.println("*********** Free memory **********");
                    npostings = 0; // new partial index
                }
                if(docCounter == 10000) {

                    System.out.println(" II: " + invertedIndex.getInvertedIndex().get("the"));

                    storeIndexAndVocabularyIntoDisk(invertedIndex);
                    return;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void freeMemory(){
        System.out.println("* Freeing memory\n");
        dt.getDocIdToDocElem().clear();
        lexicon.getTermToTermStat().clear();
        invertedIndex.getInvertedIndex().clear();
    }

    public static DocumentTable getDt() {
        return dt;
    }

    public static void setDt(DocumentTable dt) {
        DataStructureHandler.dt = dt;
    }
}
