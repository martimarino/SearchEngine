package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static it.unipi.dii.aide.mircv.utils.FileSystem.skip_channel;

public class SkipElem {

    public static final int SKIPPING_INFO_SIZE = 3 * Long.BYTES + 2 * Integer.BYTES;

    private long maxDocId;          // max docid of the block
    private long docIdOffset;       // docid offset of the first posting of the block
    private long freqOffset;        // termfreq offset of the first posting of the block
    private int docIdBlockLen;      // docid block len
    private int termFreqBlockLen;   // termFreq block len

    public SkipElem() {

    }

    public SkipElem(long maxDocId, long docIdOffset, long freqOffset, int termFreqBlockLen, int docIdBlockLen) {
        this.maxDocId = maxDocId;
        this.docIdOffset = docIdOffset;
        this.freqOffset = freqOffset;
        this.docIdBlockLen = docIdBlockLen;
        this.termFreqBlockLen = termFreqBlockLen;
    }

    public long getMaxDocId() {
        return maxDocId;
    }

    public long getDocIdOffset() {
        return docIdOffset;
    }

    public long getFreqOffset() {
        return freqOffset;
    }


    public void storeSkipElemToDisk() throws IOException {

        MappedByteBuffer skipPointsBuffer = skip_channel.map(FileChannel.MapMode.READ_WRITE, skip_channel.size(), SKIPPING_INFO_SIZE);

        skipPointsBuffer.putLong(this.maxDocId);
        skipPointsBuffer.putLong(this.docIdOffset);
        skipPointsBuffer.putLong(this.freqOffset);
        skipPointsBuffer.putInt(this.docIdBlockLen);
        skipPointsBuffer.putInt(this.termFreqBlockLen);

    }

    @Override
    public String toString() {
        return "SkipElem{" +
                "maxDocId=" + maxDocId +
                ", docIdOffset=" + docIdOffset +
                ", freqOffset=" + freqOffset +
                '}';
    }

    public void setMaxDocId(long maxDocId) {
        this.maxDocId = maxDocId;
    }

    public void setDocIdOffset(long docIdOffset) {
        this.docIdOffset = docIdOffset;
    }

    public void setFreqOffset(long freqOffset) {
        this.freqOffset = freqOffset;
    }

    public int getDocIdBlockLen() {
        return docIdBlockLen;
    }

    public int getTermFreqBlockLen() {
        return termFreqBlockLen;
    }

    public void setDocIdBlockLen(int docIdBlockLen) {
        this.docIdBlockLen = docIdBlockLen;
    }

    public void setTermFreqBlockLen(int termFreqBlockLen) {
        this.termFreqBlockLen = termFreqBlockLen;
    }

}
