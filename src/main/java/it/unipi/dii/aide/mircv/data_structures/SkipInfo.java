package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static it.unipi.dii.aide.mircv.utils.Constants.SKIP_FILE;
import static it.unipi.dii.aide.mircv.utils.FileSystem.skip_channel;

public class SkipInfo {

    public static final int SKIPPING_INFO_SIZE = 3 * Long.BYTES;

    private long maxDocId;      // max docid of the block
    private long docIdOffset;   // docid offset of the first posting of the block
    private long freqOffset;    // termfreq offset of the first posting of the block

    public SkipInfo() {

    }

    public SkipInfo(long maxDocId, long docIdOffset, long freqOffset) {
        this.maxDocId = maxDocId;
        this.docIdOffset = docIdOffset;
        this.freqOffset = freqOffset;
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


    public void storeSkipInfoToDisk() throws IOException {

        MappedByteBuffer skipPointsBuffer = skip_channel.map(FileChannel.MapMode.READ_WRITE, skip_channel.size(), SKIPPING_INFO_SIZE);

        skipPointsBuffer.putLong(this.maxDocId);
        skipPointsBuffer.putLong(this.docIdOffset);
        skipPointsBuffer.putLong(this.freqOffset);

    }

    @Override
    public String toString() {
        return "SkipInfo{" +
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
}
