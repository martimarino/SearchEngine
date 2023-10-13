package it.unipi.dii.aide.mircv.data_structures;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static it.unipi.dii.aide.mircv.utils.Constants.DICTIONARY_FILE;
import static org.junit.jupiter.api.Assertions.*;

class DictionaryElemTest {

    @Test
    void readDictElem() {
        RandomAccessFile dictFile = null;
        try {
            dictFile = new RandomAccessFile(DICTIONARY_FILE, "rw");
            FileChannel outDictionaryChannel = dictFile.getChannel();
            DictionaryElem de = new DictionaryElem();
            de.readDictionaryElemFromDisk(0, outDictionaryChannel);
            System.out.println("dictionary: " + de.toString());
            DictionaryElem de1 = new DictionaryElem();
            de1.readDictionaryElemFromDisk(56, outDictionaryChannel);
            System.out.println("dictionary 1: " + de1.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}