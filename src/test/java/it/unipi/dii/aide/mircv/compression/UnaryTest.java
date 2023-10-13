package it.unipi.dii.aide.mircv.compression;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class UnaryTest {
    @Test
    void checkCompression() {
        ArrayList<Integer> toCompress = new ArrayList<>();
        toCompress.add(12);
        toCompress.add(35);
        toCompress.add(204);
        toCompress.add(30531);
        toCompress.add(21);
        byte[] compressed = Unary.integersCompression(toCompress);
        ArrayList<Integer> decompressed = Unary.integersDecompression(compressed, 5);
        for(int i = 0; i < decompressed.size(); i++)
            assertEquals(toCompress.get(i), decompressed.get(i));
    }
}