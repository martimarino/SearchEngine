//package it.unipi.dii.aide.mircv.data_structures;
//
//import java.io.*;
//import java.util.*;
//
//public class IndexMerger {
//    private final String[] BLOCK_FILES; // Paths to the block files
//    private final String MERGED_INDEX_FILE; // Path to the merged index file
//
//    public IndexMerger(String[] blockFiles, String mergedIndexFile) {
//        this.BLOCK_FILES = blockFiles;
//        this.MERGED_INDEX_FILE = mergedIndexFile;
//    }
//
//    public void mergeIndexes() {
//        // Open all block files and maintain read buffers
//        List<BufferedReader> blockReaders = new ArrayList<>();
//        for (String blockFile : BLOCK_FILES) {
//            try {
//                blockReaders.add(new BufferedReader(new FileReader(blockFile)));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        // Prepare the output writer for the merged index
//        BufferedWriter mergedIndexWriter;
//        try {
//            mergedIndexWriter = new BufferedWriter(new FileWriter(MERGED_INDEX_FILE));
//        } catch (IOException e) {
//            e.printStackTrace();
//            return;
//        }
//
//        // Create a priority queue to track the lowest termid
//        PriorityQueue<TermAndBlock> priorityQueue = new PriorityQueue<>(Comparator.comparingInt(t -> t.termId));
//
//        // Initialize the priority queue with the first term from each block
//        for (int i = 0; i < BLOCK_FILES.length; i++) {
//            try {
//                String line = blockReaders.get(i).readLine();
//                if (line != null) {
//                    String[] parts = line.split("\t"); // Assuming tab-separated terms
//                    int termId = Integer.parseInt(parts[0]);
//                    priorityQueue.add(new TermAndBlock(termId, i, line));
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        // Multi-way merge
//        while (!priorityQueue.isEmpty()) {
//            // Get the lowest term from the queue
//            TermAndBlock lowestTerm = priorityQueue.poll();
//            try {
//                mergedIndexWriter.write(lowestTerm.termLine);
//                mergedIndexWriter.newLine();
//
//                // Read the next term from the corresponding block and add it to the queue
//                String nextLine = blockReaders.get(lowestTerm.blockIndex).readLine();
//                if (nextLine != null) {
//                    priorityQueue.add(new TermAndBlock(lowestTerm.blockIndex, nextLine));
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        // Close readers and writer
//        for (BufferedReader reader : blockReaders) {
//            try {
//                reader.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        try {
//            mergedIndexWriter.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("Merged index is saved in " + MERGED_INDEX_FILE);
//    }
//
//
//    public static void main(String[] args) {
//        // Usage example:
//        String[] blockFiles = {/* Paths to your block files */};
//        String mergedIndexFile = /* Path to your merged index file */;
//
//        IndexMerger indexMerger = new IndexMerger(blockFiles, mergedIndexFile);
//        indexMerger.mergeIndexes();
//    }
//}
