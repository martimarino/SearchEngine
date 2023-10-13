package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.utils.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartialIndexBuilderTest {

    @Test
    void name() {

        Constants.COLLECTION_PATH = "src/main/resources/small_collection.tar.gz";
        PartialIndexBuilder.SPIMIalgorithm();
        
    }
}