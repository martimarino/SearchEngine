package it.unipi.dii.aide.mircv.data_structures;

public class DocumentElement {
    //single document element with correspondent docid and relative length
    private String docno;
    private int doclength;

    // constructor
    public DocumentElement(String docno, int doclength) {
        this.docno = docno;
        this.doclength = doclength;
    }

    public int getDoclength() {
        return doclength;
    }

    public void setDoclength(int doclength) {
        this.doclength = doclength;
    }

    public String getDocno() {
        return docno;
    }

    public void setDocno(String docno) {
        this.docno = docno;
    }
}
