package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.data_structures.SkipElem.SKIPPING_INFO_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.SKIP_FILE;
import static it.unipi.dii.aide.mircv.utils.FileSystem.skip_channel;

public class SkipList {

    private final ArrayList<SkipElem> arr_skipElem;
    private final Iterator<SkipElem> skipElemIterator;

    private SkipElem currSkipElem;

    private double maxTFIDF;
    private double maxBM25;

    public SkipList(long offset, int nSkipBlocks) throws IOException {

        try (
                RandomAccessFile skipFile = new RandomAccessFile(SKIP_FILE, "rw")
        ) {
            skip_channel = skipFile.getChannel();
            MappedByteBuffer skipPointsBuffer = skip_channel.map(FileChannel.MapMode.READ_ONLY, offset , SKIPPING_INFO_SIZE + (long) nSkipBlocks * SKIPPING_INFO_SIZE);

            this.arr_skipElem = new ArrayList<>();

            for (int i = 0; i < nSkipBlocks; i++) {
                SkipElem skipElem = new SkipElem();
                skipElem.setMaxDocId(skipPointsBuffer.getLong());
                skipElem.setDocIdOffset(skipPointsBuffer.getLong());
                skipElem.setFreqOffset(skipPointsBuffer.getLong());
                skipElem.setDocIdBlockLen(skipPointsBuffer.getInt());
                skipElem.setTermFreqBlockLen(skipPointsBuffer.getInt());
                arr_skipElem.add(skipElem);
            }
            this.skipElemIterator = arr_skipElem.iterator();
            currSkipElem = skipElemIterator.next();
        }
    }

    public boolean next() {

        if(skipElemIterator.hasNext()) {
            currSkipElem = skipElemIterator.next();
            return true;
        }
        currSkipElem = null;
        return false;
    }

    public SkipElem getCurrSkipElem() {
        return currSkipElem;
    }

    public Iterator<SkipElem> getSkipElemIterator() {
        return skipElemIterator;
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
