package it.unipi.dii.aide.mircv.compression;

import it.unipi.dii.aide.mircv.data_structures.Posting;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.utils.Constants.printDebug;
import static it.unipi.dii.aide.mircv.utils.Constants.verbose;

// compression of the field term frequency of the index using Unary
public class Unary {

    public static byte[] intToUnary(ArrayList<Integer> termFreqToCompress) {

        int numBits = 0;

        for (int i = 0; i < termFreqToCompress.size(); i++)
            numBits += termFreqToCompress.get(i);

        int numBytes = (int) Math.ceil((numBits / 8)) + (numBits%8 == 0? 0 : 1);
        byte[] compressedResult = new byte[numBytes];

        int byteToWrite = 0;
        int bitToWrite = 7; //start from most significant bit

        for (int i = 0; i < termFreqToCompress.size(); i++) {
            int num = termFreqToCompress.get(i);
            //System.out.println("num: " + num);
            for (int j = 0; j < num-1; j++) {
                //set correspondent bit to 1 using OR bit a bit
                compressedResult[byteToWrite] = (byte) (compressedResult[byteToWrite] | (1 << bitToWrite));

                bitToWrite--;

                // check if all byte written
                if (bitToWrite < 0) {
                    byteToWrite++; // go to next byte
                    bitToWrite = 7;
                }

            }
            bitToWrite--;

            if(bitToWrite < 0)
            {
                byteToWrite++; // go to next byte
                bitToWrite = 7;
            }

        }
/*
        for (byte b : compressedResult) {
           System.out.print(Integer.toBinaryString(b & 0xFF) + " ");
        }*/
        //unaryToInt(compressedResult);
        integerArrayDecompression(compressedResult, 3);
        return compressedResult;

    }

    public static ArrayList<Integer> integerArrayDecompression(byte[] compressedArray, int totNum) {
        ArrayList<Integer> decompressedList = new ArrayList<>();
        int currentBit = 7; // Parti dal bit più significativo nel primo byte
        int currentValue = 0;
        int nIntegers = 0;
        int currentByte = 0;
        //System.out.println("size: " + totNum);
        for (int i = 0; i < compressedArray.length; i++) {

            byte currentByteValue = compressedArray[i];

            while (currentBit >= 0) {
                // Leggi il bit corrente
                int bit = (currentByteValue >> currentBit) & 1;

                if (bit == 1) {
                    // Se il bit è 1, incrementa il valore corrente
                    currentValue++;
                } else {
                    currentValue++;
                    // Se il bit è 0, aggiungi il valore corrente alla lista decompressa
                    decompressedList.add(currentValue);
                    currentValue = 0; // Resetta il valore corrente
                    nIntegers++;
                    if(nIntegers == totNum) {
   /*                     for (Integer integ : decompressedList)
                            System.out.println("int: " + integ);
                        System.out.println("end");*/
                        return decompressedList;
                    }
                }

                currentBit--;

                // Controlla se hai letto tutti i bit in questo byte
                if (currentBit < 0) {
                    currentByte++;
                    currentBit = 7; // Vai al bit più significativo del prossimo byte
                    break;
                }
            }
        }
        return decompressedList;
    }

    public static void storeCompressedPostingIntoDisk(ArrayList<Posting> pl, FileChannel termfreqChannel, FileChannel docidChannel){

        ArrayList<Integer> tf = new ArrayList<>();
        ArrayList<Integer> docid  = new ArrayList<>();
        //number of postings in the posting list
        for(Posting ps : pl) {
            tf.add(ps.getTermFreq());
            docid.add(ps.getDocId());
        }

        byte[] compressedTf = Unary.intToUnary(tf);
        byte[] compressedDocId = VariableBytes.IntegersCompression(docid);
        // Create buffers for docid and termfreq
        try {
            MappedByteBuffer buffertermfreq = termfreqChannel.map(FileChannel.MapMode.READ_WRITE, termfreqChannel.size(), compressedTf.length); //from 0 to number of postings * int dimension
            MappedByteBuffer bufferdocid = docidChannel.map(FileChannel.MapMode.READ_WRITE, docidChannel.size(), compressedDocId.length); //from 0 to number of postings * int dimension

            for(int i = 0; i < compressedTf.length; i++)
                buffertermfreq.put(compressedTf[i]);

            for(int i = 0; i < compressedDocId.length; i++)
                bufferdocid.put(compressedDocId[i]);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static ArrayList<Posting> readCompressedPostingListFromDisk(long offsetDocId, long offsetTermFreq, int posting_size, FileChannel docidChannel, FileChannel termfreqChannel) {

        ArrayList<Posting> uncompressed = new ArrayList<>();
        byte[] docids = new byte[posting_size];
        byte[] tf = new byte[posting_size];

        try {
            MappedByteBuffer docidBuffer = docidChannel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, posting_size);
            MappedByteBuffer termfreqBuffer = termfreqChannel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, posting_size);

            //while nr of postings read are less than the number of postings to read (all postings of the term)
            for (int i = 0; i < posting_size; i++) {
                byte docid = docidBuffer.get();           // read the DocID
                byte termfreq = termfreqBuffer.get();     // read the TermFrequency
                docids[i] = docid;
                tf[i] = termfreq;
                if(verbose)
                    System.out.println("Posting list taken from disk -> " + uncompressed);
            }
            ArrayList<Integer> uncompressedTf = Unary.integerArrayDecompression(tf, posting_size);
            ArrayList<Integer> uncompressedDocid = VariableBytes.IntegersDecompression(docids);
            for(int i = 0; i < uncompressedDocid.size(); i++) {
                System.out.println("docid: " + uncompressedDocid.get(i) + " tf: " + uncompressedTf.get(i));
                uncompressed.add(new Posting(uncompressedDocid.get(i), uncompressedTf.get(i))); // add the posting to the posting list
            }
            return uncompressed;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
