//package it.unipi.dii.aide.mircv.utils;
//
//import java.io.*;
//import java.io.IOException;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.ArrayList;
//
//public class IOHandler {
//
//    RandomAccessFile dict_raf, docid_raf,
//
//    public static <T> T parametrizzata(T valore) {
//        return valore;
//    }
//
//
//
//    // Funzione generica per memorizzare dati su disco
//    private static void storeDataIntoDisk(ArrayList<?> data, FileChannel channel) {
//        try {
//            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), data.size() * getElementSize(data));
//
//            for (Object element : data) {
//                writeElementToBuffer(buffer, element);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // Funzione generica per leggere dati da disco
//    private static ArrayList<?> readDataFromDisk(FileChannel channel, int elementSize) {
//        ArrayList<Object> data = new ArrayList<>();
//
//        try {
//            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
//
//            while (buffer.hasRemaining()) {
//                Object element = readElementFromBuffer(buffer, elementSize);
//                data.add(element);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return data;
//    }
//
//    // Funzione per ottenere la dimensione di un elemento
//    private static int getElementSize(Object element) {
//        // Calcola la dimensione dell'elemento in byte
//        // Implementa questa logica in base ai tuoi requisiti
//        return /* dimensione in byte */;
//    }
//
//    // Funzione per scrivere un elemento nel buffer
//    private static void writeElementToBuffer(MappedByteBuffer buffer, Object element) {
//        // Scrivi l'elemento nel buffer
//        // Implementa questa logica in base ai tuoi requisiti
//    }
//
//    // Funzione per leggere un elemento dal buffer
//    private static Object readElementFromBuffer(MappedByteBuffer buffer, int elementSize) {
//        // Leggi l'elemento dal buffer
//        // Implementa questa logica in base ai tuoi requisiti
//        return /* elemento letto */;
//    }
//
//    // Esempio di utilizzo per memorizzare un ArrayList di stringhe
//    public static void storeStringDataIntoDisk(ArrayList<String> stringData, String filePath) {
//        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
//             FileChannel channel = raf.getChannel()) {
//            storeDataIntoDisk(stringData, channel);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // Esempio di utilizzo per leggere un ArrayList di stringhe da disco
//    public static ArrayList<String> readStringDataFromDisk(String filePath) {
//        ArrayList<String> stringData = new ArrayList<>();
//        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r");
//             FileChannel channel = raf.getChannel()) {
//            stringData = (ArrayList<String>) readDataFromDisk(channel, /* dimensione di un elemento String */);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return stringData;
//    }
//
//}
