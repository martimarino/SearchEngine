package it.unipi.dii.aide.mircv.data_structures;

public class Posting {
    private final int docId; //delta code
    private final int termFreq; //unary code

    /**
     * Create a posting with a specified document ID.
     *
     * @param docId The document ID associated with the posting.
     */

    public Posting(int docId, int termFreq) {
        this.docId = docId;
        this.termFreq = termFreq;
    }

    public int getDocId() { return docId; }

    public int getTermFreq() { return termFreq; }
}