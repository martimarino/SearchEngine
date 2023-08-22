package it.unipi.dii.aide.mircv.data_structures;

/**
 *  Stores unique terms and their statistics
 */
public class DictionaryElem {

    private String term;
    private int df;         // document frequency, number of documents in which there is the term
    private int cf;         // collection frequency, number of occurrences of the term in the collection
    private int termId;
    private long offsetTermFreq; // starting point of the posting list of the term in the term freq file
    private long offsetDocId; // starting point of the posting list of the term in the docid file

    public DictionaryElem(String term, int df, int cf, int termId, long offsetTermFreq, long offsetDocId) {
        this.term = term;
        this.df = df;
        this.cf = cf;
        this.termId = termId;
        this.term = term;
        this.offsetTermFreq = offsetTermFreq;
        this.offsetDocId = offsetDocId;
    }

    // constructor without parameters
    public DictionaryElem() {
        this.df = 0;
        this.cf = 0;
        this.termId = 0;
        this.term = "";
    }

    /**
        Constructor with TermID parameter. Called when the first occurrence of a term is found.
        Is the first occurrence found of the term in the collection, will be in at least one document and present
        once in the collection for these set df and cf to 1.
     */
    public DictionaryElem(int termId, String term) {
        this.df = 1;                // set to 1
        this.cf = 1;                // set to 1
        this.termId = termId;
        this.term = term;
    }

    // method to increment of 1 the document frequency
    public void incDf(){
        df++;
    }

    // method to increment of 1 the collection frequency
    public void incCf(){
        cf++;
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