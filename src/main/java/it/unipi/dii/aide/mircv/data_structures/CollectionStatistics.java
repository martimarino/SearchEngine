package it.unipi.dii.aide.mircv.data_structures;

public class CollectionStatistics {
    int nDocs; //number of documents in the collection
    double avgDocLen;

    public double getAvgDocLen() {
        return avgDocLen;
    }

    public void setAvgDocLen(double avgDocLen) {
        this.avgDocLen = avgDocLen;
    }

    public CollectionStatistics() {
        this.nDocs = 0;
        this.avgDocLen = 0;
    }

    public int getnDocs() {
        return nDocs;
    }

    public void setnDocs(int nDocs) {
        this.nDocs = nDocs;
    }


}
