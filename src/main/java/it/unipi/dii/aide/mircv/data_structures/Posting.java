package it.unipi.dii.aide.mircv.data_structures;

import java.util.Objects;

public class Posting {

    private final int docId;    // DocID (recommended delta code compression)
    private int termFreq;       // frequency of the term in the document(recommended unary code compression)


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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Posting posting = (Posting) o;
        return ((docId == posting.docId) && (termFreq == posting.termFreq));
    }
}