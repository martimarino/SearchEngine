package it.unipi.dii.aide.mircv.test;

import it.unipi.dii.aide.mircv.compression.Unary;
import it.unipi.dii.aide.mircv.compression.VariableBytes;
import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

import java.io.*;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.*;
import static it.unipi.dii.aide.mircv.data_structures.DocumentElem.DOCELEM_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.termFreq_channel;

public class TestFunctionalities {

    static String document1 = "0\tTest functionalities of the project";
    static String document2 = "1\tFunction to test the project";
    static String document3 = "2\tCheck the project functionalities";
    private static SkipList sl;

    public static void main(String[] args) {

        // Test compression
        // checkUnaryCompression();
        //checkVariableBytesCompression();

        //checkDocumentTable();
        try {
            checkPostingsWithSkip();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    static void checkUnaryCompression() {
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
        ArrayList<Integer> decompressed = Unary.integersDecompression(compressed, toCompress.size());
        for (int i = 0; i < toCompress.size(); i++)
            assert (toCompress.get(i) == decompressed.get(i));
    }

    static void checkVariableBytesCompression() {
        ArrayList<Integer> toCompress = new ArrayList<>();
        toCompress.add(1);
        toCompress.add(1);
        toCompress.add(2);
        toCompress.add(1);
        toCompress.add(35);
        toCompress.add(204);
        toCompress.add(30531);
        toCompress.add(1);
        byte[] compressed = VariableBytes.integersCompression(toCompress);
        ArrayList<Integer> decompressed = VariableBytes.integersDecompression(compressed);
        for (int i = 0; i < decompressed.size(); i++)
            assert (toCompress.get(i) == decompressed.get(i));
    }

    static void checkDocumentTable() throws IOException {

        String DOCTABLE_FILE = RES_FOLDER + "test/documentTable"; // file in which is stored the document table
        RandomAccessFile documentTableFile = new RandomAccessFile(DOCTABLE_FILE, "rw");
        docTable_channel = documentTableFile.getChannel();
        ArrayList<String> docs = new ArrayList<>();
        docs.add(document1);
        docs.add(document2);
        docs.add(document3);
        int docCounter = 1;
        for (String doc : docs) {
            // preprocess document text
            ArrayList<String> preprocessed = null;
            try {
                preprocessed = TextProcessor.preprocessText(doc);

                String docno = preprocessed.remove(0);
                // check if document is empty
                if (preprocessed.isEmpty() || (preprocessed.size() == 1 && preprocessed.get(0).isEmpty()))
                    continue;

                DocumentElem de = new DocumentElem(docno, docCounter, preprocessed.size());
                documentTable.put(docCounter, de);      // add current Doc into Document Table in memory
                docCounter++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        storeDocumentTableIntoDisk();

        for (int i = 0; i < docTable_channel.size(); i += DOCELEM_SIZE) {
            DocumentElem de = new DocumentElem();
            de.readDocumentElementFromDisk(i, docTable_channel); // get the ith DocElem
            assert (de.equals(documentTable.get(de.getDocid())));
        }

    }

    static void checkPostingsWithSkip() throws IOException {

        RandomAccessFile docId = new RandomAccessFile(DOCID_FILE, "rw");
        docId_channel = docId.getChannel();
        RandomAccessFile tf = new RandomAccessFile(TERMFREQ_FILE, "rw");
        termFreq_channel = tf.getChannel();
        Flags.setConsiderSkipElem(true);
        Flags.setCompression(true);
        System.out.println(DictionaryElem.getDictElemSize());

        ArrayList<Posting> postingList = new ArrayList<>();
        ArrayList<Posting> postingListFromFile;
        Dictionary dict = new Dictionary();
        dict.readDictionaryFromDisk();
        DictionaryElem de  = dict.getTermStat("berlin");
        System.out.println("tf: " + de.getDf());
        postingListFromFile = readPostingFromFile();
        SkipList sl = new SkipList(de.getSkipListOffset(), de.getSkipListLen());

        while(sl.getCurrSkipElem() != null) {
            postingList.addAll(readCompressedPostingListFromDisk(sl.getCurrSkipElem().getDocIdOffset(), sl.getCurrSkipElem().getFreqOffset(), sl.getCurrSkipElem().getTermFreqBlockLen(), sl.getCurrSkipElem().getDocIdBlockLen()));
            sl.next();
        }

        System.out.println("posting from disk size: " + postingListFromFile.size() + " posting size: " + postingList.size());

        for(int i = 0; i < postingList.size(); i++){
            assert (postingList.get(i).equals(postingListFromFile.get(i)));
            if(!postingList.get(i).equals(postingListFromFile.get(i)))
                System.out.println("Lists not equals");
        }
    }

        static ArrayList<Posting> readPostingFromFile(){

        ArrayList<Posting> postingList = new ArrayList<>();
        String filePath = DEBUG_FOLDER + "berlin_test.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
        String line;
        br.readLine();
        while ((line = br.readLine()) != null) {
            String[] parts = line.split(" ");
            if (parts.length >= 2) {
            int docId = Integer.parseInt(parts[1]);
            int tf = Integer.parseInt(parts[3]);

            Posting p = new Posting(docId, tf);
            postingList.add(p);
            }

        }
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
            return postingList;

        }
}

