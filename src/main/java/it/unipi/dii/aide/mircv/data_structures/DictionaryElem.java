package it.unipi.dii.aide.mircv.data_structures;

/**
 *  Stores unique terms and their statistics
 */
public class DictionaryElem {

    private static final int DICT_ELEM_SIZE = DataStructureHandler.TERM_DIM + 3 * Integer.BYTES + 2 * Long.BYTES;

    private String term;        //32 byte
    private int df;             // document frequency, number of documents in which there is the term
    private int cf;             // collection frequency, number of occurrences of the term in the collection
    private int termId;
    private long offsetTermFreq;// starting point of the posting list of the term in the term freq file
    private long offsetDocId;   // starting point of the posting list of the term in the docid file

    // constructor with all parameters
    public DictionaryElem(String term, int df, int cf, int termId, long offsetTermFreq, long offsetDocId) {
        this.setTerm(term);
        this.setDf(df);
        this.setCf(cf);
        this.setTermId(termId);
        this.setTerm(term);
        this.setOffsetTermFreq(offsetTermFreq);
        this.setOffsetDocId(offsetDocId);
    }

    // constructor without parameters
    public DictionaryElem() {
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
        this.setDf(1);                // set to 1
        this.setCf(1);                // set to 1
        this.setTermId(termId);
        this.setTerm(term);
    }

    public static int getDictElemSize() {
        return DICT_ELEM_SIZE;
    }

    // method to increment of 1 the document frequency
    public void incDf(){
        setDf(getDf() + 1);
    }

    // method to increment of 1 the collection frequency
    public void incCf(){
        setCf(getCf() + 1);
    }

    // add the quantity passed as a parameter to the current Df
    public void addDf(int n)
    {
        setDf(getDf() + 1);
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

}
