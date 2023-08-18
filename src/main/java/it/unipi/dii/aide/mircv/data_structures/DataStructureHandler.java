package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.Main;
import it.unipi.dii.aide.mircv.TextProcessor;
import jdk.jfr.Description;

import java.beans.BeanProperty;
import java.io.*;
import java.lang.annotation.Documented;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;


public class DataStructureHandler {

    // data structures init
    static DocumentTable dt = new DocumentTable();
    static Lexicon lexicon = new Lexicon();
    static InvertedIndex invertedIndex = new InvertedIndex();
    static final String documentFile = "src/main/resources/document.txt";
    static final int docnodim  = 20;

    public static void getCollectionFromDisk() {

    }

    public static void getFlagsFromDisk() {

    }
   /**
    @param start offset of the document reading from document file

    **/

    public static DocumentElement getDocumentIndexFromDisk(int start) {

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
        return null;
    }

    public static void getDictionaryFromDisk() {

    }

    public static void storeCollectionIntoDisk(){

    }

    public static void storeFlagsIntoDisk(){

    }

    // Read document table elements from file
    public static void storeDocumentTableElementIntoDisk(DocumentElement de){

        System.out.println("DOCNO: " + de.getDocno() + " DOCID: " + de.getDocid() + " DOCLENGTH: " + de.getDoclength());

        try (FileChannel channel = new RandomAccessFile(documentFile, "rw").getChannel()) {
            int docsize = 4 + docnodim + 4; // Size in bytes of docid, docno, and doclength
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), docsize);
            // Buffer not created
            if(buffer == null)
                return;
            //allocate 20bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(20);
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


    public static void storeDictionaryIntoDisk(){

    }

    /**
     * Function to create and fill the lexicon, document table and inverted index
     * @throws IOException
     */
    public static void initializeDataStructures() throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.collection_path), StandardCharsets.UTF_8))) {

            System.out.println("\n*** Data structure build ***\n");     // print for the User Interface

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
                storeDocumentTableElementIntoDisk(de);
                //dt.setDocIdToDocElem(docno, docCounter, preprocessed.size());       // add element to document table
                docCounter++;              // update DocID counter
                if(docCounter == 10000)
                    return;
                // scroll through the term of the document
                for (String term : preprocessed) {
                    // Lexicon build
                    LexiconElem lexElem = lexicon.getOrCreateTerm(term, termCounter);
                    termCounter++;         // update TermID counter

                    // Build inverted index
                    // "addTerm" add posting in inverted index and return true if term is in a new doc -> update df
                    if (invertedIndex.addTerm(term, docCounter)) {
                        lexElem.incDf();                // increment Document Frequency of the term in the lexicon
                    }
                 // test print for lexicon
                    /*if (termCounter < 10) {
                        HashMap<String, LexiconElem> lex = lexicon.getTermToTermStat();
                        System.out.println("********** Lexicon **********");
                        System.out.println("Term: " + term);
                        System.out.println("TermId: " + termCounter + " - TermId elem:" + lex.get(term).getTermId());
                        System.out.println("Df: " + lex.get(term).getDf());
                        System.out.println("Cf: " + lex.get(term).getCf());
                        System.out.println("Lexicon size: " + lex.size());
                    }*/
               }
  /*              // test print for documentElement
                if (docCounter == 53) {
                    HashMap<Integer, DocumentElement> doctable = dt.getDocIdToDocElem();
                    System.out.println("********** Document Table **********");
                    System.out.println("Docid: " + docCounter);
                    System.out.println("DocTable size: " + doctable.size());
                    System.out.println("Docno: " + doctable.get(docCounter - 1).getDocno());
                    System.out.println("Length: " + doctable.get(docCounter - 1).getDoclength());*/
                //}
            }
        }
    }

    public static DocumentTable getDt() {
        return dt;
    }

    public static void setDt(DocumentTable dt) {
        DataStructureHandler.dt = dt;
    }
}
