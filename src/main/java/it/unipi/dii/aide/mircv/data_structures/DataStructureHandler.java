package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.compression.Unary;
import it.unipi.dii.aide.mircv.compression.VariableBytes;
import it.unipi.dii.aide.mircv.query.scores.Score;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.data_structures.DocumentElem.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;


/**
 * This class handles the storage and retrieval of data structures used for document indexing.
 */
public final class DataStructureHandler {

    public static HashMap<Integer, DocumentElem> documentTable = new HashMap<>();     // hash table DocID to related DocElement
    public static Dictionary dictionary;                                                 // dictionary in memory
    public static HashMap<String, ArrayList<Posting>> invertedIndex;                     // hash table Term to related Posting list
    public static ArrayList<Long> dictionaryBlockOffsets = new ArrayList<>();            // Offsets of the dictionary blocks


    // function to store the whole document table into disk
    public static void storeDocumentTableIntoDisk() {

        try {
            MappedByteBuffer buffer = docTable_channel.map(FileChannel.MapMode.READ_WRITE, docTable_channel.size(), (long) DOCELEM_SIZE * documentTable.size());

            // Buffer not created
            if(buffer == null)
                return;
            // scan all document elements of the Document Table
            for(DocumentElem de: documentTable.values()) {
                //allocate bytes for docno
                CharBuffer charBuffer = CharBuffer.allocate(DOCNO_DIM);

                //put every char into charbuffer
                for (int i = 0; i < de.getDocno().length(); i++)
                    charBuffer.put(i, de.getDocno().charAt(i));

                // write docno, docid and doclength into document file
                buffer.put(StandardCharsets.UTF_8.encode(charBuffer));
                buffer.putInt(de.getDocid());
                buffer.putInt(de.getDoclength());

                if(Flags.isDebug_flag())
                    saveIntoFile(" docno: " + de.getDocno() + " docID: " + de.getDocid() + " length: " + de.getDoclength(), "document_table_debug.txt");

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    // function to store Dictionary and Inverted Index into disk
    public static void storeIndexAndDictionaryIntoDisk() {

        try {
            dictionary.sort();              // Sort the dictionary lexicographically
            dictionaryBlockOffsets.add(PARTIAL_DICTIONARY_OFFSET);// update of the offset of the block for the dictionary file

            // iterate through all the terms of the dictionary ordered
            for (String term : dictionary.getTermToTermStat().keySet()) {

                //get posting list of the term
                ArrayList<Posting> posList = invertedIndex.get(term);

                //create dictionary element for the term
                DictionaryElem dictElem = dictionary.getTermStat(term);
                dictElem.setOffsetTermFreq(INDEX_OFFSET);
                dictElem.setOffsetDocId(INDEX_OFFSET);

                // Create buffers for docid and termfreq
                MappedByteBuffer buffer_docid = partialDocId_channel.map(FileChannel.MapMode.READ_WRITE, partialDocId_channel.size(), (long) posList.size() * Integer.BYTES); // from 0 to number of postings * int dimension
                MappedByteBuffer buffer_termfreq = partialTermFreq_channel.map(FileChannel.MapMode.READ_WRITE, partialTermFreq_channel.size(), (long) posList.size() * Integer.BYTES); //from 0 to number of postings * int dimension

                // iterate through all the postings of the posting list
                for (Posting posting : posList) {
                    // Buffer not created
                    if (buffer_docid == null || buffer_termfreq == null)
                        return;

                    buffer_docid.putInt(posting.getDocId());         // write DocID
                    buffer_termfreq.putInt(posting.getTermFreq());   // write TermFrequency

                    if(Flags.isDebug_flag()) {
                        saveIntoFile(dictElem.getTerm() + ": " + posting, "spimi_pl.txt");
                        saveIntoFile(dictElem.getTerm() + ": " + posting.getDocId(), "spimi_docid.txt");
                        saveIntoFile(dictElem.getTerm() + ": " + posting.getTermFreq(), "spimi_tf.txt");
                    }

                    INDEX_OFFSET += Integer.BYTES;
                }

                // store dictionary entry to disk
                dictElem.storeDictionaryElemIntoDisk(partialDict_channel);
            }
            System.out.println(dictionary.getTermToTermStat().size() + " terms stored in block " + (dictionaryBlockOffsets.size()-1));

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }


    // store one posting list of a term into the disk
    public static double[] storePostingListIntoDisk(ArrayList<Posting> pl, double idf) {

        //number of postings in the posting list
        int len = pl.size();
        double scoreBM25 = 0;
        double scoreTFIDF = 0;

        double currentScoreBM25 = 0;
        double currentScoreTFIDF = 0;

        double[] score = new double[2];
        
        StringBuilder debug_pl = new StringBuilder();
        StringBuilder debug_docid = new StringBuilder();

        try {
            // Create buffers for docid and termfreq
            MappedByteBuffer bufferdocid = docId_channel.map(FileChannel.MapMode.READ_WRITE, docId_channel.size(), (long) len*Integer.BYTES); // from 0 to number of postings * int dimension
            MappedByteBuffer buffertermfreq = termFreq_channel.map(FileChannel.MapMode.READ_WRITE, termFreq_channel.size(), (long) len*Integer.BYTES); //from 0 to number of postings * int dimension

            for (Posting posting : pl) {
                bufferdocid.putInt(posting.getDocId());
                buffertermfreq.putInt(posting.getTermFreq());
                if(Flags.isDebug_flag()) {
                    debug_pl.append("{").append(posting.getDocId()).append(", ").append(posting.getTermFreq()).append("} ");
                    debug_docid.append(posting.getDocId()).append( ", ");
                }

                currentScoreBM25 = Score.computeBM25(idf, posting);
                currentScoreTFIDF = Score.computeTFIDF(idf, posting);

                if(currentScoreBM25 > scoreBM25)
                    scoreBM25 = currentScoreBM25;

                if(currentScoreTFIDF > scoreTFIDF)
                    scoreTFIDF = currentScoreTFIDF;
            }

            if (Flags.isDebug_flag()) {
                saveIntoFile(debug_pl.toString(), "merge_pl.txt");
                saveIntoFile(debug_docid.toString(), "merge_docid.txt");
            }

            score[0] = scoreBM25;
            score[1] = scoreTFIDF;
            return score;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    // function to read all document table from disk and put it in memory (HashMap documentTable)
    public static void readDocumentTableFromDisk() {

        System.out.println("Loading document table from disk...");

        try (RandomAccessFile docTableFile = new RandomAccessFile(DOCTABLE_FILE, "r")) {

            docTable_channel = docTableFile.getChannel();

            // for to read all DocumentElement stored into disk
            for (int i = 0; i < docTable_channel.size(); i += DOCELEM_SIZE) {
                DocumentElem de = new DocumentElem();
                de.readDocumentElementFromDisk(i, docTable_channel); // get the ith DocElem
                documentTable.put(de.getDocid(), de);
            }

        }catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }


    /**
     * function to read and return a posting list from disk
     *
     * @param offsetDocId       offset of the DocID
     * @param offsetTermFreq    offset of the Term Frequency
     * @param posting_size      size of the posting list
     * @param di_channel        channel where to read docids
     * @param tf_channel        channel where to read termfreq
     * @return the posting lists read from disk
     */
    public static ArrayList<Posting> readPostingListFromDisk(long offsetDocId, long offsetTermFreq, int posting_size, FileChannel di_channel, FileChannel tf_channel) {

        ArrayList<Posting> pl = new ArrayList<>();

        MappedByteBuffer docidBuffer;
        MappedByteBuffer termfreqBuffer;

        try {

            docidBuffer = di_channel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, (long) posting_size * Integer.BYTES);
            termfreqBuffer = tf_channel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, (long) posting_size * Integer.BYTES);

            //while nr of postings read are less than the number of postings to read (all postings of the term)
            for (int i = 0; i < posting_size; i++) {
                int docid = docidBuffer.getInt();           // read the DocID
                int termfreq = termfreqBuffer.getInt();     // read the TermFrequency
                pl.add(new Posting(docid, termfreq)); // add the posting to the posting list
            }
            return pl;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * function to store posting list after compression into disk
     *
     * @param pl                posting list to store
     * @return Term Frequency and DocID compressed length
     */

    public static int[] storeCompressedPostingIntoDisk(ArrayList<Posting> pl){

        // ArrayList per memorizzare le frequenze dei termini (tf) e gli ID dei documenti (docid)
        ArrayList<Integer> tf = new ArrayList<>();
        ArrayList<Integer> docid  = new ArrayList<>();
        // array di interi per memorizzare le lunghezze dei dati compressi
        int[] length = new int[2];

        // Estrai le frequenze dei termini e gli ID dei documenti dai Posting nell'ArrayList
        for(Posting ps : pl) {
            tf.add(ps.getTermFreq());
            docid.add(ps.getDocId());
        }

        // Comprimi i dati delle frequenze dei termini e degli ID dei documenti
        byte[] compressedTf = Unary.integersCompression(tf);
        byte[] compressedDocId = VariableBytes.integersCompression(docid);
        // Create buffers for docid and termfreq
        try {
            // Crea MappedByteBuffer per memorizzare i dati compressi nei FileChannels
            MappedByteBuffer buffertermfreq = termFreq_channel.map(FileChannel.MapMode.READ_WRITE, termFreq_channel.size(), compressedTf.length); //number of bytes of compressed tfs
            MappedByteBuffer bufferdocid = docId_channel.map(FileChannel.MapMode.READ_WRITE, docId_channel.size(), compressedDocId.length); //number of bytes of compressed docids

            // Scrivi i dati compressi nei buffer
            buffertermfreq.put(compressedTf);
            bufferdocid.put(compressedDocId);

            // Memorizza le lunghezze dei dati compressi nell'array di lunghezze
            length[0] = compressedTf.length;
            length[1] = compressedDocId.length;
            return length;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
    /**
     * function to store posting list after compression into disk
     *
     * @param offsetDocId       offset from where to start the read of the DocID values
     * @param offsetTermFreq    offset from where to start the read of the Term Frequency values
     * @param termFreqSize      size of the compressed Term Frequency values
     * @param docIdSize         size of the compressed DocID values
     * @return termfreq and docid compressed length
     */
    public static ArrayList<Posting> readCompressedPostingListFromDisk(long offsetDocId, long offsetTermFreq, int termFreqSize, int docIdSize) {

        // ArrayList vuota per memorizzare i posting decompressi
        ArrayList<Posting> uncompressed = new ArrayList<>();
        // due array di byte per memorizzare i dati compressi letti dai file
        byte[] docids = new byte[docIdSize];
        byte[] tf = new byte[termFreqSize];

        try {
            // due oggetti MappedByteBuffer per leggere i dati dal disco utilizzando i FileChannels
            MappedByteBuffer docidBuffer = docId_channel.map(FileChannel.MapMode.READ_ONLY, offsetDocId, docIdSize);
            MappedByteBuffer termfreqBuffer = termFreq_channel.map(FileChannel.MapMode.READ_ONLY, offsetTermFreq, termFreqSize);

            // legge i dati compressi nei byte array
            termfreqBuffer.get(tf, 0, termFreqSize);
            docidBuffer.get(docids, 0, docIdSize);

            // ecomprime i dati utilizzando metodi personalizzati
            ArrayList<Integer> uncompressedDocid = VariableBytes.integersDecompression(docids);
            ArrayList<Integer> uncompressedTf = Unary.integersDecompression(tf, uncompressedDocid.size());
            // itera attraverso i dati decompressi e crea oggetti Posting
            for(int i = 0; i < uncompressedTf.size(); i++) {
                uncompressed.add(new Posting(uncompressedDocid.get(i), uncompressedTf.get(i))); // add the posting to the posting list
            }
            // restituisce l'ArrayList contenente i posting decompressi
            return uncompressed;

        } catch (IOException e) {
            e.printStackTrace();
        }
        // in caso di errore
        return null;
    }

}
