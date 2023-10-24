package it.unipi.dii.aide.mircv.data_structures;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static it.unipi.dii.aide.mircv.utils.Constants.SKIP_FILE;
import static it.unipi.dii.aide.mircv.utils.FileSystem.skip_channel;

public class SkipInfo {

    public static final int SKIPPING_INFO_SIZE = 3 * Long.BYTES + Integer.BYTES;

    private long maxDocId;      // max docid of the block
    private long docIdOffset;   // docid offset of the first posting of the block
    private long freqOffset;    // termfreq offset of the first posting of the block

    private int nPostings;

    public SkipInfo() {

    }

    public SkipInfo(long maxDocId, long docIdOffset, long freqOffset, int nPostings) {
        this.maxDocId = maxDocId;
        this.docIdOffset = docIdOffset;
        this.freqOffset = freqOffset;
        this.nPostings = nPostings;
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

    public int getNPostings() {
        return nPostings;
    }

    public void storeSkipInfoToDisk() throws IOException {

        MappedByteBuffer skipPointsBuffer = skip_channel.map(FileChannel.MapMode.READ_WRITE, skip_channel.size(), SKIPPING_INFO_SIZE);

        skipPointsBuffer.putLong(this.maxDocId);
        skipPointsBuffer.putLong(this.docIdOffset);
        skipPointsBuffer.putLong(this.freqOffset);
        skipPointsBuffer.putInt(this.nPostings);

    }


    public void readSkipInfoFromDisk(long start, int iter) {

        try (
                RandomAccessFile skipFile = new RandomAccessFile(SKIP_FILE, "rw")
        ) {
            skip_channel = skipFile.getChannel();
            MappedByteBuffer skipPointsBuffer = skip_channel.map(FileChannel.MapMode.READ_ONLY, start+ (long) iter *SKIPPING_INFO_SIZE, SKIPPING_INFO_SIZE);

            this.maxDocId = skipPointsBuffer.getLong();
            this.docIdOffset = skipPointsBuffer.getLong();
            this.freqOffset = skipPointsBuffer.getLong();
            this.nPostings = skipPointsBuffer.getInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "SkipInfo{" +
                "maxDocId=" + maxDocId +
                ", docIdOffset=" + docIdOffset +
                ", freqOffset=" + freqOffset +
                ", nPostings=" + nPostings +
                '}';
    }
}
