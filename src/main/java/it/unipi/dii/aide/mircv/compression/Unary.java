package it.unipi.dii.aide.mircv.compression;

import java.util.ArrayList;

// compression of the field term frequency of the index using Unary
public class Unary {

    /**
     * fuction to compress the Term Frequency values using Unary compression
     *
     * @param termFreqToCompress             ArrayList of Term Frequency of the posting list
     * @return  a byte array with the Unary compression of the input Term Frequency values
     */
    public static byte[] integersCompression(ArrayList<Integer> termFreqToCompress) {

        int numBits = 0;

        for (Integer freqToCompress : termFreqToCompress) numBits += freqToCompress;

        int numBytes = (int) Math.ceil(((double) numBits / 8)) + (numBits%8 == 0? 0 : 1);
        byte[] compressedResult = new byte[numBytes];

        int byteToWrite = 0;
        int bitToWrite = 7; //start from most significant bit

        for (int num : termFreqToCompress) {
            //System.out.println("num: " + num);
            for (int j = 0; j < num - 1; j++) {
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

            if (bitToWrite < 0) {
                byteToWrite++; // go to next byte
                bitToWrite = 7;
            }

        }
/*
        for (byte b : compressedResult) {
           System.out.print(Integer.toBinaryString(b & 0xFF) + " ");
        }*/
        //unaryToInt(compressedResult);
        //integerArrayDecompression(compressedResult, 3);
        return compressedResult;

    }
    /**
     * fuction to compress the Term Frequency values using Unary compression
     *
     * @param compressedArray           array containing the compressed Term Frequency values
     * @param totNum                    total number of integers to decompress
     * @return  an ArrayList containing the decompressed Term Frequency values
     */

    public static ArrayList<Integer> integersDecompression(byte[] compressedArray, int totNum) {
        ArrayList<Integer> decompressedList = new ArrayList<>();
        int currentBit = 7;
        int currentValue = 0;
        int nIntegers = 0;
        int currentByte = 0;

        for (byte currentByteValue : compressedArray) {

            while (currentBit >= 0) {
                // Read current bit
                int bit = (currentByteValue >> currentBit) & 1;

                if (bit == 1) {
                    // If bit is 1, increment current value
                    currentValue++;
                } else {
                    currentValue++;
                    // If bit is 0, add current value to decompressed list
                    decompressedList.add(currentValue);

                    currentValue = 0; // Reset current value
                    nIntegers++;
                    if (nIntegers == totNum) {
                        return decompressedList;
                    }
                }

                currentBit--;

                // check if all byte read
                if (currentBit < 0) {
                    currentByte++;
                    currentBit = 7; // Go to the most meaningful bit of the next byte
                    break;
                }
            }
        }
        return decompressedList;
    }

}
