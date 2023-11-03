package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

/**
 *  Stores unique terms and their statistics
 */
public class DictionaryElem {

    public static int getDictElemSize() {
        // if compression case, need to store 2 more integers (dimension of compressed DocID and Term Frequency values)
        int DICT_ELEM_SIZE = TERM_DIM + 2*Integer.BYTES + 2*Long.BYTES;
        return DICT_ELEM_SIZE + ((Flags.considerSkipInfo() && Flags.isCompressionEnabled()) ? 2*Integer.BYTES : 0)
                              + ((Flags.considerSkipInfo()) ? (4*Long.BYTES + Integer.BYTES) : 0);
    }

    private String term;        //32 byte
    private int df;             // document frequency, number of documents in which there is the term
    private int cf;             // collection frequency, number of occurrences of the term in the collection
    private long offsetTermFreq;// starting point of the posting list of the term in the term freq file
    private long offsetDocId;   // starting point of the posting list of the term in the docid file

    // compression
    private int docIdSize;      // dimension in byte of compressed docid of the posting list
    private int termFreqSize;   //dimension in byte of compressed termfreq of the posting list

    private double idf;

    //skipping
    private long skipListOffset;      // offset of the skip element
    private int skipListLen;       // how many skip blocks

    //scoring
    private double maxTFIDF;
    private double maxBM25;


    public DictionaryElem() {
        this.df = 0;
        this.cf = 0;
        this.term = "";
        this.docIdSize = 0;
        this.termFreqSize = 0;
        this.skipListOffset = -1;
        this.skipListLen = -1;
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
    }


    /**
        Constructor with TermID parameter. Called when the first occurrence of a term is found.
        Is the first occurrence found of the term in the collection, will be in at least one document and present
        once in the collection for these set df and cf to 1.
     */
    public DictionaryElem(String term) {
        this.term = term;
        this.df = 0;
        this.cf = 0;
        this.docIdSize = 0;
        this.termFreqSize = 0;
        this.skipListOffset = -1;
        this.skipListLen = -1;
        this.idf = 0;
        this.maxTFIDF = 0;
        this.maxBM25 = 0;
    }

    // add the quantity passed as a parameter to the current Df
    public void addDf(int n) { setDf(getDf() + n); }

    // add the quantity passed as a parameter to the current Cf
    public void addCf(int n) { setCf(getCf() + n); }

    // ---- start method get and set ----

    public void setDf(int df) { this.df = df; }

    public void setCf(int cf) { this.cf = cf; }

    public void setTerm(String term) { this.term = term; }

    public double getIdf() { return idf; }

    public int getDf() { return df; }

    public int getCf() { return cf; }

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

    public int getDocIdSize() {
        return docIdSize;
    }

    public void setDocIdSize(int docIdSize) {
        this.docIdSize = docIdSize;
    }

    public int getTermFreqSize() {
        return termFreqSize;
    }

    public void setTermFreqSize(int termFreqSize) {
        this.termFreqSize = termFreqSize;
    }

    public long getSkipListOffset() { return skipListOffset; }

    public void setSkipListOffset(long skipOffset) { this.skipListOffset = skipOffset; }

    public int getSkipListLen() { return skipListLen; }

    public void setSkipListLen(int skipArrLen) { this.skipListLen = skipArrLen; }

    @Override
    public String toString() {
        return "DictionaryElem{" +
                "term='" + term + '\'' +
                ", df=" + df +
                ", cf=" + cf +
                ", offsetTermFreq=" + offsetTermFreq +
                ", offsetDocId=" + offsetDocId +
                ", docIdSize=" + docIdSize +
                ", termFreqSize=" + termFreqSize +
                ", idf=" + idf +
                ", skipListOffset=" + skipListOffset +
                ", skipListLen=" + skipListLen +
                ", maxTFIDF=" + maxTFIDF +
                ", maxBM25=" + maxBM25 +
                '}';
    }

    /**
     * function to store one dictionary elem into disk
     */
    public void storeDictionaryElemIntoDisk(FileChannel channel){

        MappedByteBuffer buffer;
        try
        {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), getDictElemSize());

            // Buffer not created
            if (buffer == null)
                return;

            //allocate bytes for docno
            CharBuffer charBuffer = CharBuffer.allocate(TERM_DIM);
            //put every char into charbuffer
            for (int i = 0; i < term.length(); i++)
                charBuffer.put(i, term.charAt(i));

            // write docno, docid and doclength into document file
            buffer.put(StandardCharsets.UTF_8.encode(charBuffer));      // write term
            buffer.putInt(df);                            // write df
            buffer.putInt(cf);                            // write cf
            buffer.putLong(offsetTermFreq);               // write offset tf
            buffer.putLong(offsetDocId);                  // write offset docid
            if(Flags.considerSkipInfo()) {     // if in merge phase
                if (Flags.isCompressionEnabled()) { // need to store also the size of DocID and Term Frequency compressed values
                    buffer.putInt(termFreqSize);
                    buffer.putInt(docIdSize);
                }
                buffer.putLong(skipListOffset);
                buffer.putInt(skipListLen);
                buffer.putDouble(idf);
                buffer.putDouble(maxBM25);
                buffer.putDouble(maxTFIDF);

                if(Flags.isDebug_flag()) {
                    saveIntoFile(this.toString(), "merge_de.txt");
                }
            }

            PARTIAL_DICTIONARY_OFFSET += getDictElemSize();       // update offset

            assert !term.isEmpty();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * function to read one Dictionary Element from disk
     *
     * @param start     offset of the document reading from document file
     */
    public void readDictionaryElemFromDisk(long start){

        try {
            MappedByteBuffer buffer = partialDict_channel.map(FileChannel.MapMode.READ_ONLY, start, getDictElemSize()); // get first term of the block
            CharBuffer.allocate(TERM_DIM);              //allocate a charbuffer of the dimension reserved to docno
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

            if(!(charBuffer.toString().split("\0").length == 0))
                term = charBuffer.toString().split("\0")[0];   // read term

            buffer.position(TERM_DIM);             // skip over term position
            df = buffer.getInt();                  // read df
            cf = buffer.getInt();                  // read cf
            offsetTermFreq = buffer.getLong();     // read offset tf
            offsetDocId = buffer.getLong();        // read offset docid
            if(Flags.considerSkipInfo()) {
                skipListOffset = buffer.getLong();
                skipListLen = buffer.getInt();
                idf = buffer.getDouble();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void setIdf(double idf){
        this.idf = idf;
    }

    public double computeIdf() {return Math.log10(CollectionStatistics.getNDocs() / (double)this.df);}

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }

}
