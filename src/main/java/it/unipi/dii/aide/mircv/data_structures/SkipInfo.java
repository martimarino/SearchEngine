package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SkipInfo {

    public static final int SKIPPING_INFO_SIZE = 2 * Integer.BYTES + 3 * Long.BYTES;

    private long maxDocId;

    private long docIdOffset;
    private int docIdBlockLen;
    private long freqOffset;
    private int freqBlockLen;


    public SkipInfo(long maxDocId, long docIdOffset, int docIdBlockLen, long freqOffset, int freqBlockLen) {
        this.maxDocId = maxDocId;
        this.docIdOffset = docIdOffset;
        this.docIdBlockLen = docIdBlockLen;
        this.freqOffset = freqOffset;
        this.freqBlockLen = freqBlockLen;
    }

    public long getMaxDocId() {
        return maxDocId;
    }

    public void setMaxDocId(long maxDocId) {
        this.maxDocId = maxDocId;
    }

    public long getDocIdOffset() {
        return docIdOffset;
    }

    public void setDocIdOffset(long docIdOffset) {
        this.docIdOffset = docIdOffset;
    }

    public int getDocIdBlockLen() {
        return docIdBlockLen;
    }

    public void setDocIdBlockLen(int docIdBlockLen) {
        this.docIdBlockLen = docIdBlockLen;
    }

    public long getFreqOffset() {
        return freqOffset;
    }

    public void setFreqOffset(long freqOffset) {
        this.freqOffset = freqOffset;
    }

    public int getFreqBlockLen() {
        return freqBlockLen;
    }

    public void setFreqBlockLen(int freqBlockLen) {
        this.freqBlockLen = freqBlockLen;
    }

    @Override
    public String toString() {
        return "SkipInfo{" +
                "maxDocId=" + maxDocId +
                ", docIdOffset=" + docIdOffset +
                ", docIdBlockLen=" + docIdBlockLen +
                ", freqOffset=" + freqOffset +
                ", freqBlockLen=" + freqBlockLen +
                '}';
    }

    public void storeSkipInfoToDisk(FileChannel skipFileChannel) throws IOException {

        ByteBuffer skipPointsBuffer = ByteBuffer.allocate(SKIPPING_INFO_SIZE);
        skipFileChannel.position(skipFileChannel.size());

        skipPointsBuffer.putLong(this.maxDocId);
        skipPointsBuffer.putLong(this.docIdOffset);
        skipPointsBuffer.putInt(this.docIdBlockLen);
        skipPointsBuffer.putLong(this.freqOffset);
        skipPointsBuffer.putInt(this.freqBlockLen);

        skipPointsBuffer = ByteBuffer.wrap(skipPointsBuffer.array());

        while(skipPointsBuffer.hasRemaining())
            skipFileChannel.write(skipPointsBuffer);
    }

    public void readSkippingInfoFromDisk(long start, FileChannel skipFileChannel) throws IOException {
        ByteBuffer skipPointsBuffer = ByteBuffer.allocate(SKIPPING_INFO_SIZE);

        skipFileChannel.position(start);

        while (skipPointsBuffer.hasRemaining())
            skipFileChannel.read(skipPointsBuffer);

        skipPointsBuffer.rewind();
        this.setMaxDocId(skipPointsBuffer.getLong());
        this.setDocIdOffset(skipPointsBuffer.getLong());
        this.setDocIdBlockLen(skipPointsBuffer.getInt());
        this.setFreqOffset(skipPointsBuffer.getLong());
        this.setFreqBlockLen(skipPointsBuffer.getInt());
    }

}
