package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.utils.Constants.*;

/**
 *  Stores unique terms and their statistics
 */
public class DictionaryElem {

    static final int DICT_ELEM_SIZE = TERM_DIM + 3 * Integer.BYTES + 2 * Long.BYTES; // 20 + 12 + 16 = 48

    private String term;        //32 byte
    private int df;             // document frequency, number of documents in which there is the term
    private int cf;             // collection frequency, number of occurrences of the term in the collection
    private int termId;
    private long offsetTermFreq;// starting point of the posting list of the term in the term freq file
    private long offsetDocId;   // starting point of the posting list of the term in the docid file


    DictionaryElem() {
        this.setDf(0);
        this.setCf(0);
        this.setTermId(0);
        this.setTerm("");
    }

    /**
        Constructor with TermID parameter. Called when the first occurrence of a term is found.
        Is the first occurrence found of the term in the collection, will be in at least one document and present
        once in the collection for these set df and cf to 1.
     */
    public DictionaryElem(int termId, String term) {
        this.setDf(0);
        this.setCf(0);
        this.setTermId(termId);
        this.setTerm(term);
    }

    // add the quantity passed as a parameter to the current Df
    public void addDf(int n)
    {
        setDf(getDf() + n);
    }

    // add the quantity passed as a parameter to the current Cf
    public void addCf(int n)
    {
        setCf(getCf() + n);
    }

    // ---- start method get and set ----
    public void setDf(int df) { this.df = df; }

    public void setCf(int cf) { this.cf = cf; }

    public void setTerm(String term) { this.term = term; }

    public void setTermId(int termId) { this.termId = termId; }

    public int getDf() { return df; }

    public int getCf() { return cf; }

    public int getTermId() { return termId; }

    public String getTerm() {
        return term;
    }

    public long getOffsetTermFreq() {
        return offsetTermFreq;
    }

    public void setOffsetTermFreq(long offsetTermFreq) {
        this.offsetTermFreq = offsetTermFreq;
    }

    public long getOffsetDocId() {
        return offsetDocId;
    }

    public void setOffsetDocId(long offsetDocId) {
        this.offsetDocId = offsetDocId;
    }

    @Override
    public String toString() {
        return "DictionaryElem{" +
                "term='" + term + '\'' +
                ", df=" + df +
                ", cf=" + cf +
                ", termId=" + termId +
                ", offsetTermFreq=" + offsetTermFreq +
                ", offsetDocId=" + offsetDocId +
                '}';
    }

    /**
     * function to store one dictionary elem into disk
     *
     * @param channel   indicate the file where to write
     */
    void storeDictionaryElemIntoDisk(FileChannel channel){
        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), DICT_ELEM_SIZE);

            // Buffer not created
            if(buffer == null)
                return;
            //allocate bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(TERM_DIM);
            //put every char into charbuffer
            for(int i = 0; i < term.length(); i++)
                charBuffer.put(i, term.charAt(i));
            // write docno, docid and doclength into document file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));      // write term
            buffer.putInt(df);                            // write Df
            buffer.putInt(cf);                            // write Cf
            buffer.putInt(termId);                        // write TermID
            buffer.putLong(offsetTermFreq);               // write offset Tf
            buffer.putLong(offsetDocId);                  // write offset DID
            PARTIAL_DICTIONARY_OFFSET += DICT_ELEM_SIZE;        // update offset

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
    public void readDictionaryElemFromDisk(long start, FileChannel channel){

        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start,  DICT_ELEM_SIZE); // get first term of the block
            CharBuffer.allocate(TERM_DIM);              //allocate a charbuffer of the dimension reserved to docno
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            if(!(charBuffer.toString().split("\0").length == 0))
                term = charBuffer.toString().split("\0")[0];   // read term

            buffer.position(TERM_DIM);                  // skip over term position
            df = buffer.getInt();                  // read Df
            cf = buffer.getInt();                  // read Cf
            termId = buffer.getInt();              // read TermID
            offsetTermFreq = buffer.getLong();     // read offset Tf
            offsetDocId = buffer.getLong();        // read offset DID
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // function to show in console a posting list passed by parameter
    public static void printPostingList(ArrayList<Posting> pl, String term)
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

}
