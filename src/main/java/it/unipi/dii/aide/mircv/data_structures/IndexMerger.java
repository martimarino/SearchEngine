package it.unipi.dii.aide.mircv.data_structures;

import java.io.*;
import java.util.*;

public class IndexMerger {

    private static PriorityQueue<TermBlock> pq = new PriorityQueue<>(dictionaryBlocks.size() == 0 ? 1 : dictionaryBlocks.size(), new CompareTerm());

    public IndexMerger() {

    }

    public static void mergeBlocks() {

        //get blocks from file
        //open all files in read for each block (leggo da offset blocco fino a offset blocco successivo; nell'ultimo fino a channel.size())
        //open one file in output for the index
        // 4. pick all the first terms of each block and order them basing on the lexicographic order and block index
        // get from ordered list the first two (if exists) elements with the same term
        // svuoto list ordinata del punto precedente
        //aggiorno posizione in lettura nel buffer dei blocchi da cui si è letto
        // rieseguo dal punto 4.
        //finchè non è terminata la lettura di tutti i blocchi (terminata la lettura del buffer)
        DataStructureHandler.getBlocksFromDisk();

        MappedByteBuffer buffer;
        try (FileChannel channel = new RandomAccessFile(DataStructureHandler.VOCABULARY_FILE, "rw").getChannel(); FileChannel outchannel = new RandomAccessFile(DataStructureHandler.VOCABULARY_FILE, "w").getChannel()) {
            for(int i = 0; i <= DataStructureHandler.dictionaryBlocks.size(); i++) {
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, DataStructureHandler.dictionaryBlocks.get(i), TERM_DIM);
                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to docno
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                pq.add(new TermBlock(charBuffer.toString().split("\0")[0], i)); //split using end string character
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }




    private static class TermBlock {

        String term;
        int block;

        public TermBlock(String term, int block) {
            this.term = term;
            this.block = block;
        }

        public void setTerm(String term) {
            this.term = term;
        }

        public void setBlock(int block) {
            this.block = block;
        }

        public String getTerm() {
            return term;
        }

        public int getBlock() {
            return block;
        }

    }

    private static class CompareTerm implements java.util.Comparator<TermBlock> {
        @Override
        public int compare(TermBlock o1, TermBlock o2) {
            if (o1.getTerm().compareTo(o2.getTerm()) == 0) {
                if(o1.getBlock() < o2.getBlock())
                    return -1;
                else
                    return 1;
            } else {
                return o1.getTerm().compareTo(o2.getTerm());
            }
        }
    }

}
