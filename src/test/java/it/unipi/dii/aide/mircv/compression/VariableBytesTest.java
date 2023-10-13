package it.unipi.dii.aide.mircv.compression;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class VariableBytesTest {

    @Test
    void checkCompression() {
        ArrayList<Integer> toCompress = new ArrayList<>();
        toCompress.add(12);
        toCompress.add(35);
        toCompress.add(2040);
        toCompress.add(1);
        toCompress.add(3031);
        toCompress.add(21);
        byte[] compressed = VariableBytes.integersCompression(toCompress);
        ArrayList<Integer> decompressed = VariableBytes.integersDecompression(compressed);
        for(int i = 0; i < decompressed.size(); i++) {
            assertEquals(toCompress.get(i), decompressed.get(i));
            System.out.println(decompressed.get(i));
        }
    }
}