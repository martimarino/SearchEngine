package it.unipi.dii.aide.mircv.index;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.utils.Constants.*;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.utils.TextProcessor;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.*;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;



public final class PartialIndexBuilder {

    /**
     * Implements the SPIMI algorithm for indexing large collections.
     */
    public static void SPIMI() {

        dictionary = new Dictionary();
        invertedIndex = new HashMap<>();

        long memoryAvailable = (long) (Runtime.getRuntime().maxMemory() * MEMORY_THRESHOLD);
        int docCounter = 1;         // counter for DocID
        int totDocLen = 0;          // variable for the sum of the lengths of all documents

        File file = new File(COLLECTION_PATH);
        try (
                final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)))
        ) {

            // read from compressed collection
            TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
            BufferedReader buffer_collection;
            if(tarArchiveEntry == null)
                return;
            buffer_collection = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));

            String record;

            while ((record = buffer_collection.readLine()) != null) {

                // check for malformed line, no \t
                int separator = record.indexOf("\t");
                if (record.isBlank() || separator == -1)  // empty string or composed by whitespace characters or malformed
                    continue;

                // preprocess document text
                ArrayList<String> preprocessed = TextProcessor.preprocessText(record);
                String docno = preprocessed.remove(0);

                // check if document is empty
                if (preprocessed.isEmpty() || (preprocessed.size() == 1 && preprocessed.get(0).isEmpty()))
                    continue;

                DocumentElem de = new DocumentElem(docno, docCounter, preprocessed.size());
                documentTable.put(docCounter, de);      // add current Doc into Document Table in memory
                totDocLen += preprocessed.size();       // add current document length

                // scan all term in the current document
                for (String term : preprocessed) {
                    // check if current term length is greater than the maximum allowed
                    if(term.length() > TERM_DIM)
                        term = term.substring(0,TERM_DIM);                  // truncate term

                    assert !term.isEmpty();

                    DictionaryElem dictElem = dictionary.getOrCreateTerm(term);

                    if(addTerm(term, docCounter))
                        dictElem.addDf(1);
                    dictElem.addCf(1);

//                    if(!termList.contains(term))
//                        termList.add(term);
                }

                docCounter++;       // update DocID counter

                if(Runtime.getRuntime().totalMemory() > memoryAvailable){
                    printDebug("********** Memory full **********");

                    storeIndexAndDictionaryIntoDisk();  //store index and dictionary to disk
                    storeDocumentTableIntoDisk(); // store document table one document at a time for each block

                    freeMemory();
                }
            }

            if (!dictionary.getTermToTermStat().isEmpty()) {
                storeIndexAndDictionaryIntoDisk();  //store index and dictionary to disk
                storeDocumentTableIntoDisk(); // store document table one document at a time for each block
                freeMemory();
            }

            //DataStructureHandler.storeBlockOffsetsIntoDisk();

            CollectionStatistics.setNDocs(docCounter);        // set total number of Document in the collection
            CollectionStatistics.setTotDocLen(totDocLen);     // set the sum of the all document length in the collection
            CollectionStatistics.storeCollectionStatsIntoDisk();         // store collection statistics into disk

//            printDebug("termCounter = " + termList.size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * Add the current term to the inverted index
     * @return false if the term has been already encountered in the current document,
     *         true if the term has been encountered for the first time in the current document or if the term was for the first time encountered
     * ***/
    private static boolean addTerm(String term, int docId) {

        // Get or create the PostingList associated with the term
        if(!invertedIndex.containsKey(term))
            invertedIndex.put(term, new ArrayList<>());

        ArrayList<Posting> pl = invertedIndex.get(term);
        int pl_size = pl.size();

        // Check if the posting list is empty or if the last posting is for a different document
        if (pl.isEmpty() || pl.get(pl_size - 1).getDocId() != docId) {
            // Add a new posting for the current document
            pl.add(new Posting(docId, 1));
            return true; // Increment df only if it's a new document
        } else {
            // Increment the term frequency for the current document
            pl.get(pl_size - 1).addTermFreq(1);
            return false; // No need to increment df
        }
    }

    /***
     * Method to free memory by deleting the information in partial document table, dictionary and inverted index.
     * It also calls Java garbage collector
     */
    private static void freeMemory(){
        documentTable.clear();
        dictionary.getTermToTermStat().clear();
        invertedIndex.clear();
        System.gc();
        printDebug("********** Free memory **********");
    }

}
