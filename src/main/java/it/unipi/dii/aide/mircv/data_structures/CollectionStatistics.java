package it.unipi.dii.aide.mircv.data_structures;

public class CollectionStatistics {

    int nDocs; //number of documents in the collection
    double totDocLen;

    public double getTotDocLen() {
        return totDocLen;
    }

    public void setTotDocLen(double totDocLen) {
        this.totDocLen = totDocLen;
    }

    public CollectionStatistics() {
        this.nDocs = 0;
        this.totDocLen = 0;
    }

    public int getnDocs() {
        return nDocs;
    }

    public void setnDocs(int nDocs) {
        this.nDocs = nDocs;
    }


}
