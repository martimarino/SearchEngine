package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.data_structures.SkipInfo.SKIPPING_INFO_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.SKIP_FILE;
import static it.unipi.dii.aide.mircv.utils.FileSystem.skip_channel;

public class SkipList {

    private final ArrayList<SkipInfo> arr_skipInfo;
    private final Iterator<SkipInfo> skipInfoIterator;

    private SkipInfo currSkipInfo;

    private double maxTFIDF;
    private double maxBM25;


    public SkipList(long offset, int nSkipBlocks) throws IOException {

        try (
                RandomAccessFile skipFile = new RandomAccessFile(SKIP_FILE, "rw")
        ) {
            skip_channel = skipFile.getChannel();
            MappedByteBuffer skipPointsBuffer = skip_channel.map(FileChannel.MapMode.READ_ONLY, offset , SKIPPING_INFO_SIZE + (long) nSkipBlocks * SKIPPING_INFO_SIZE);

            this.arr_skipInfo = new ArrayList<>();

            for (int i = 0; i < nSkipBlocks; i++) {
                SkipInfo skipInfo = new SkipInfo();
                skipInfo.setMaxDocId(skipPointsBuffer.getLong());
                skipInfo.setDocIdOffset(skipPointsBuffer.getLong());
                skipInfo.setFreqOffset(skipPointsBuffer.getLong());
                arr_skipInfo.add(skipInfo);
            }

            this.skipInfoIterator = arr_skipInfo.iterator();
            currSkipInfo = skipInfoIterator.next();
        }
    }

    public boolean next() {

        if(skipInfoIterator.hasNext()) {
            currSkipInfo = skipInfoIterator.next();
            return true;
        }
        currSkipInfo = null;
        return false;
    }

    public SkipInfo getCurrSkipInfo() {
        return currSkipInfo;
    }

    public ArrayList<SkipInfo> getArr_skipInfo() {
        return arr_skipInfo;
    }

    public Iterator<SkipInfo> getSkipInfoIterator() {
        return skipInfoIterator;
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
