package it.unipi.dii.aide.mircv.data_structures;

public class DocumentElement {

    //single document element with correspondent docid and relative length
    private int docid;
    private String docno;
    private int doclength;


    public DocumentElement(String docno, int docid, int doclength) {
        this.docno = docno;
        this.doclength = doclength;
        this.docid = docid;
    }

    public DocumentElement() {
        this.docno = "";
        this.docid = 0;
        this.doclength = 0;
    }


    // ---- start method get and set ----

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

    public int getDocid() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

}
