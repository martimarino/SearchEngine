package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.utils.Constants;
import org.junit.jupiter.api.Test;

class PartialIndexBuilderTest {

    @Test
    void name() {
        Constants.MEMORY_THRESHOLD = 0.008;
        Constants.COLLECTION_PATH = "src/test/resources/small_collection.tar.gz";
        PartialIndexBuilder.SPIMIalgorithm();

    }
}