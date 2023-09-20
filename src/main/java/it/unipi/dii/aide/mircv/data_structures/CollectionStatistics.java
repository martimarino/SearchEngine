package it.unipi.dii.aide.mircv.data_structures;

/**
 * class to contain the statistics of the collection
 */
public class CollectionStatistics {
    int nDocs;          // number of documents in the collection
    double totDocLen;   // sum of the all document length in the collection

    // constructor without parameters, set nDocs and totDocLen to 0
    public CollectionStatistics() {
        this.nDocs = 0;
        this.totDocLen = 0;
    }

    // set and get methods
    public double getTotDocLen() {
        return totDocLen;
    }

    public void setTotDocLen(double totDocLen) {
        this.totDocLen = totDocLen;
    }

    public int getnDocs() {
        return nDocs;
    }

    public void setnDocs(int nDocs) {
        this.nDocs = nDocs;
    }
}
