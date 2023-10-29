package it.unipi.dii.aide.mircv.compression;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class UnaryTest {
    @Test
    void checkCompression() {
        ArrayList<Integer> toCompress = new ArrayList<>();
        toCompress.add(1);
        toCompress.add(1);
        toCompress.add(2);
        toCompress.add(1);
        toCompress.add(35);
        toCompress.add(204);
        toCompress.add(30531);
        toCompress.add(1);
        byte[] compressed = Unary.integersCompression(toCompress);
/*        for(int i = 0; i < compressed.length; i++)
            System.out.println("i " + i + " : "  + compressed[i]);*/
        ArrayList<Integer> decompressed = Unary.integersDecompression(compressed, toCompress.size());
        for(int i = 0; i < toCompress.size(); i++)
            //assertEquals(toCompress.get(i), decompressed.get(i));
            System.out.println(toCompress.get(i) + "   " + decompressed.get(i) );
    }
}