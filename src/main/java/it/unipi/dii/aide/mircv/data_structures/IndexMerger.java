package it.unipi.dii.aide.mircv.data_structures;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.*;
import static it.unipi.dii.aide.mircv.data_structures.DictionaryElem.DICT_ELEM_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.delete_tempFiles;

/**
 * class to merge the InverteIndex
 */
public class IndexMerger {

    private static PriorityQueue<TermBlock> pq = new PriorityQueue<>(dictionaryBlockOffsets.size() == 0 ? 1 : dictionaryBlockOffsets.size(), new CompareTerm());

    public IndexMerger() {

    }

    static int i = 0;

    // abaco 1 2 4
    // alveare 3
    // abaco 1 abaco 2 alveare 3 abaco 4
    // abaco 1 abaco 2 abaco 4 alveare 3

    /**
     *  function to merge the block of the inverted index
     */
    public static void mergeBlocks() {

        System.out.println("Merging partial files...");

        // 1. get blocks of dictionary from file
        // 2. open all files in read for each block (leggo da offset 1 elemento; nell'ultimo fino a channel.size())
        // 3. open one file in output for the index (risultato finale) scriviamo informazioni da lista di c
        // 4. pick all the first terms of each block and order them basing on the lexicographic order and block index
        //    -> si mette nella lista di candidati, si dovrà mantenere informazione per ogni termine in quale blocco lo
        //       si è letto (per aggiornamenti futuri della posizione di lettura)
        // 5.  merge delle posting list degli indici parziali dei termini nel relativo blocco
        //    -> (avremo lista con candidati senza ripetizioni di termini)
        // 6. prendo dalla lista candidati(ordinata) primo term in ordine lessicografico e scrivo nel file output
        //      (se tutto ordinato correttamente si avrà certezza che term sarà con valori finali e non ci sarà da aggiornarlo
        // 7. aggiorno posizione in lettura nel buffer dei blocchi da cui si è letto il primo termine della lista candidati (passo sopra)
        // 7.1. aggiorno lista candidati, tolgo il primo termine.
        // 7.2. aggiorno il vocabolario per quel termine e scrivo su file
        // 8. rieseguo dal punto 4. :
        //      finchè non è terminata la lettura di tutti i blocchi (terminata la lettura del buffer)
        //      finchè non è vuota la lista dei candidati

        int nrBlocks = DataStructureHandler.dictionaryBlockOffsets.size();    // dictionary number
        DataStructureHandler.readBlockOffsetsFromDisk(); // 1: get offsets of dictionary blocks from disk

        MappedByteBuffer buffer;

        ArrayList<Long> currentBlockOffset = new ArrayList<>(nrBlocks); // array containing the current read offset for each block
        currentBlockOffset.addAll(dictionaryBlockOffsets);


        try (
                RandomAccessFile partialDocidFile = new RandomAccessFile(PARTIAL_DOCID_FILE, "rw");
                RandomAccessFile partialTermfreqFile = new RandomAccessFile(PARTIAL_TERMFREQ_FILE, "rw");
                RandomAccessFile partialDictFile = new RandomAccessFile(PARTIAL_DICTIONARY_FILE, "rw");

                RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile dictFile = new RandomAccessFile(DICTIONARY_FILE, "rw");

                // 2: open channels for reading the partial vocabulary file, the output index file and the output vocabulary file
                FileChannel dictChannel = partialDictFile.getChannel();
                FileChannel docidChannel = partialDocidFile.getChannel();
                FileChannel termfreqChannel = partialTermfreqFile.getChannel();
                // 3: open the file in output for the index
                FileChannel outDictionaryChannel = dictFile.getChannel();
                FileChannel outDocIdChannel = docidFile.getChannel();
                FileChannel outTermFreqChannel = termfreqFile.getChannel();
        ) {

            //scroll all blocks
            for(int i = 0; i <  nrBlocks; i++) {
                buffer = dictChannel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(i), TERM_DIM); //map current block in memory
                String term = StandardCharsets.UTF_8.decode(buffer).toString().split("\0")[0];  // get first term of the block
                pq.add(new TermBlock(term, i));                                                       // add to the priority queue a TermBlock element (term + its blocks number)
            }

            if(verbose){
                System.out.println("\nINITIAL PQ: ");
                for (TermBlock elemento : pq) {
                    System.out.println(elemento.term + " - " + elemento.block);
                }
            }

            // build temp structures
            DictionaryElem tempDE = new DictionaryElem();       // empty temporary DictionaryELem, contains the accumulated data for each term
            ArrayList<Posting> tempPL = new ArrayList();             // empty temporary PostingList

            DictionaryElem currentDE;
            ArrayList<Posting> currentPL;

            int lim = 20;

            TermBlock currentTermBlock;
            String term = "";
            int block_id = -1;

            //5: merging the posting list
            while(!pq.isEmpty()) {
                if (verbose && i < lim) System.out.println("-----------------------------------------------------");

                // get first element from priority queue
                currentTermBlock = pq.poll();        // get lowest term
                term = currentTermBlock.getTerm();
                block_id = currentTermBlock.getBlock();

                if(term.equals("00"))
                    System.out.println("TERM: " + term + " BLOCK: " + block_id + " OFFSET: " + currentBlockOffset.get(block_id));

                if (verbose && i < lim) System.out.println("Current term (removed from pq): " + currentTermBlock);

                //read new element
                if (!(currentBlockOffset.get(block_id) + DICT_ELEM_SIZE >= dictChannel.size())) {
                    buffer = dictChannel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(block_id) + DICT_ELEM_SIZE, TERM_DIM); // get first term of the block
                    String[] t = StandardCharsets.UTF_8.decode(buffer).toString().split("\0");      // 4: add the first term and block number to priority queue
                    if (!(t.length == 0))
                        pq.add(new TermBlock(t[0], block_id)); //add to the priority queue a term block element (term + its blocks number)
                    if (verbose && i < lim)
                        System.out.println("New term (added to pq) -> TERM: " + Arrays.toString(t) + " - BLOCK: " + block_id);
                }


                if (verbose && i < lim) {
                    System.out.println("ACTUAL PQ: ");
                    for (TermBlock elemento : pq) {
                        System.out.println(elemento.term + " - " + elemento.block);
                    }
                }
                long startTime = System.currentTimeMillis();

                // get current elem of dictionary
                currentDE = readDictionaryElemFromDisk(currentBlockOffset.get(block_id), dictChannel);

                endTime = System.currentTimeMillis();
                if(i >= 20000)
                    System.out.println(ANSI_CYAN + "\nreadDictionaryElemFromDisk in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ") for i : " + i  +"channel size: " + dictChannel.size() + ANSI_RESET);

                startTime = System.currentTimeMillis();

                // get current postings
                currentPL = readPostingListFromDisk(currentDE.getOffsetDocId(), currentDE.getOffsetTermFreq(), term, currentDE.getDf(), docidChannel, termfreqChannel);

                endTime = System.currentTimeMillis();

                if(i % 1000 == 0)
                    System.out.println(ANSI_CYAN + "\nreadDictionaryElemFromDisk in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ") for i : " + i + ANSI_RESET);
                if(verbose && i < lim) {
                    System.out.println("CURR DE: " + currentDE);
                    System.out.println("CURR PL: " + currentPL.size());
                }

                if (tempDE.getTerm().equals("")) {        // first iteration
                    if(verbose && i < 10) System.out.println("*** First iteration");

                    //set temp variables values
                    tempDE = currentDE;
                    tempDE.setOffsetTermFreq(outTermFreqChannel.size());
                    tempDE.setOffsetDocId(outDocIdChannel.size());
                    tempPL = currentPL;

                    if(verbose && i < lim) {
                        System.out.println("TEMP DE: " + tempDE);
                        System.out.println("TEMP PL: " + tempPL.size());
                    }
                } else {
                    if(verbose && i < lim) {
                        System.out.println("TEMP DE: " + tempDE);
                        System.out.println("TEMP PL: " + tempPL.size());
                    }

                    if (currentDE.getTerm().equals(tempDE.getTerm())) { // same term found, temporary structures update
                        if(verbose && i < lim) System.out.println("*** Same term of previous one -> accumulate on temp variables");

                        // update DictionaryElem
                        tempDE.addCf(currentDE.getCf());
                        tempDE.addDf(currentDE.getDf());

                        assert tempPL != null;
                        tempPL.addAll(currentPL);

                        /****** da chiedere ******
                         * nell'estensione della postingList non si deve controllare duplicati perchè scorriamo i
                         * documenti quindi in blocchi diversi ci saranno dati da documenti diversi quindi in blocchi
                         * diversi dell'inverted index ci saranno postings contenenti docID diversi
                         */
                        if(verbose && i < lim) {
                            System.out.println("CURR DE: " + currentDE);
                            System.out.println("CURR PL: " + currentPL);
                            System.out.println("TEMP DE: " + tempDE);
                            System.out.println("TEMP PL: " + tempPL);
                        }
                    }
                    else{    // write to disk
                        if(verbose && i < lim) System.out.println("*** Writing elem to disk...");

                        if(verbose && i < lim) {
                            System.out.println("TEMP DE: " + tempDE);
                            System.out.println("TEMP PL: " + tempPL);
                        }

                        // write DictionaryElem to disk
                        startTime = System.currentTimeMillis();
                        storeDictionaryElemIntoDisk(tempDE, outDictionaryChannel);
                        endTime = System.currentTimeMillis();
                        if(i >= 20000)
                            System.out.println(ANSI_CYAN + "\nStoreDictionaryIntoDisk in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ") for i : " + i + ANSI_RESET);


                        long startPosting = System.currentTimeMillis();

                        // write InvertedIndexElem to disk
                        storePostingListIntoDisk(tempPL, outTermFreqChannel, outDocIdChannel);

                        long endPosting = System.currentTimeMillis();
                        if (i >= 20000)
                            System.out.println(ANSI_CYAN + "\nStorePostingListIntoDisk in " + (endPosting - startPosting) + " ms (" + formatTime(startPosting, endPosting) + ") for i : " + i  + ANSI_RESET);

                        //set temp variables values

                        tempDE = currentDE;
                        tempPL = currentPL;

                        if(verbose && i < lim) {
                            System.out.println("CURR DE: " + currentDE);
                            System.out.println("CURR PL: " + currentPL.size());
                            System.out.println("TEMP DE: " + tempDE);
                            System.out.println("TEMP PL: " + tempPL.size());
                        }
                    }
                }

                currentBlockOffset.set(block_id, currentBlockOffset.get(block_id) + DICT_ELEM_SIZE);
                i++;
            }

//            delete_tempFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static class TermBlock {

        String term;
        int block;

        public TermBlock(String term, int block) {
            this.term = term;
            this.block = block;
        }

        public void setTerm(String term) {
            this.term = term;
        }

        public void setBlock(int block) {
            this.block = block;
        }

        public String getTerm() {
            return term;
        }

        public int getBlock() {
            return block;
        }

        @Override
        public String toString() {
            return "TermBlock{" +
                    "term='" + term + '\'' +
                    ", block=" + block +
                    '}';
        }
    }

    private static class CompareTerm implements Comparator<TermBlock> {
        @Override
        public int compare(TermBlock tb1, TermBlock tb2) {
            // Confronto i termini
            int termComparison = tb1.getTerm().compareTo(tb2.getTerm());

            if (termComparison == 0) {
                // Se i termini sono uguali, confronta per numero di blocco
                return Integer.compare(tb1.getBlock(), tb2.getBlock());
            }

            return termComparison;
        }
    }

}
