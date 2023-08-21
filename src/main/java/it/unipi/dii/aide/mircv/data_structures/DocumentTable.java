package it.unipi.dii.aide.mircv.data_structures;

/**
 *  Stores documents and their statistics
 */
import java.util.*;

public class DocumentTable{

    private final HashMap<Integer, DocumentElement> docIdToDocElem;     // hash table DocID to related DocElement


    public DocumentTable() {
        this.docIdToDocElem = new HashMap<>();
    }

    public DocumentTable(HashMap<Integer, DocumentElement> docIdToDocElem) {
        this.docIdToDocElem = docIdToDocElem;
    }


    // ---- start method get and set ----

    public HashMap<Integer, DocumentElement> getDocIdToDocElem() {
        return docIdToDocElem;
    }

    public void setDocIdToDocElem(String docno, int docid, int doclength){
        this.docIdToDocElem.put(docid, new DocumentElement(docno, doclength, docid));
    }
}
