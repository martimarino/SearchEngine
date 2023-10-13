package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

public class SkipList {

    private ArrayList<SkipInfo> arr_skipInfo;
    private Iterator<SkipInfo> skipInfoIterator;

    private SkipInfo currSkipInfo;

    private double maxTFIDF;
    private double maxBM25;


    public SkipList(long offset, int nSkipBlocks, FileChannel skipFileChannel) throws IOException {

        this.arr_skipInfo = new ArrayList<>();

        for(int i = 0; i < nSkipBlocks; i++) {
            SkipInfo skipInfo = new SkipInfo();
            skipInfo.readSkipInfoFromDisk(offset, skipFileChannel);
            arr_skipInfo.add(skipInfo);
        }

        this.skipInfoIterator = arr_skipInfo.iterator();
        currSkipInfo = skipInfoIterator.next();
    }

    public boolean next() {

        if(skipInfoIterator.hasNext()) {
            currSkipInfo = skipInfoIterator.next();
            return true;
        }
        currSkipInfo = null;
        return false;

    }

    //returns the score of the current posting
    public double score() {
        return 0.0;
    }


    public SkipInfo getCurrSkipInfo() {
        return currSkipInfo;
    }

    public ArrayList<SkipInfo> getArr_skipInfo() {
        return arr_skipInfo;
    }

    public void setArr_skipInfo(ArrayList<SkipInfo> arr_skipInfo) {
        this.arr_skipInfo = arr_skipInfo;
    }

    public Iterator<SkipInfo> getSkipInfoIterator() {
        return skipInfoIterator;
    }

    public void setSkipInfoIterator(Iterator<SkipInfo> skipInfoIterator) {
        this.skipInfoIterator = skipInfoIterator;
    }

    public void setCurrSkipInfo(SkipInfo currSkipInfo) {
        this.currSkipInfo = currSkipInfo;
    }

    public double getMaxTFIDF() {
        return maxTFIDF;
    }

    public void setMaxTFIDF(double maxTFIDF) {
        this.maxTFIDF = maxTFIDF;
    }

    public double getMaxBM25() {
        return maxBM25;
    }

    public void setMaxBM25(double maxBM25) {
        this.maxBM25 = maxBM25;
    }
}
