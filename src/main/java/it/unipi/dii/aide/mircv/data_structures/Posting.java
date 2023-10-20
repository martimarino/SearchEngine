package it.unipi.dii.aide.mircv.data_structures;

public class Posting {

    private final int docId;    // DocID (recommended delta code compression)
    private int termFreq;       // frequency of the term in the document(recommended unary code compression)

    private double maxTFIDF;

    private double maxBM25;

    /**
     * Create a posting with a specified document ID.
     *
     * @param docId The document ID associated with the posting.
     * @param termFreq the occurrence of term in the document associated with the posting.
     */
    public Posting(int docId, int termFreq) {
        this.docId = docId;
        this.termFreq = termFreq;
    }

    public Posting() {
        this.docId = 0;
        this.termFreq = 0;
    }

    public void addTermFreq(int n){
        this.termFreq += n;
    }

    // ---- start method get and set ----

    public int getDocId() { return docId; }

    public int getTermFreq() { return termFreq; }

    @Override
    public String toString() {
        return "Posting{" +
                "docId=" + docId +
                ", termFreq=" + termFreq +
                '}';
    }

    public double getMaxBM25() {
        return 0;
    }

    public double getMaxTFIDF() {
        return 0;
    }
}