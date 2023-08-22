package it.unipi.dii.aide.mircv.data_structures;

import java.io.*;
import java.util.*;

public class IndexMerger {
    private final String[] BLOCK_FILES; // Paths to the block files
    private final String MERGED_INDEX_FILE; // Path to the merged index file

    public IndexMerger(String[] blockFiles, String mergedIndexFile) {
        this.BLOCK_FILES = blockFiles;
        this.MERGED_INDEX_FILE = mergedIndexFile;
    }

    public void mergeIndexes() {

    }

}
