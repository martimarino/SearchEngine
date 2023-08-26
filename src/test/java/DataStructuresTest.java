import static it.unipi.dii.aide.mircv.utils.DataAnalysis.findMaxWordLength;
import static org.junit.Assert.*;

import it.unipi.dii.aide.mircv.Main;
import it.unipi.dii.aide.mircv.TextProcessor;
import it.unipi.dii.aide.mircv.data_structures.DataStructureHandler;
import it.unipi.dii.aide.mircv.data_structures.PostingList;
import org.junit.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;


public class DataStructuresTest {

    @BeforeClass
    public static void initClass() {
//        System.out.println("initClass()");
    }

    @AfterClass
    public static void endClass() {
//        System.out.println("endClass()");
    }
    @Before
    public void initMethod() throws IOException {
//        System.out.println("initMethod()");
//        Main.main(null);
    }
    @After
    public void endMethod() {
//        System.out.println("end Method");
    }

    @Ignore
    @Test
    public void test1() throws IOException {
        System.out.println("Test 1");
        //DataStructureHandler.initializeDataStructures();
    }

    @Test
    public void test2() {
//        System.out.println("Test 2");
    }


    @Test
    public void datasetTest() throws IOException {
        System.out.println("\n*** Test Dataset ***");
        HashMap<String, PostingList> dict = new HashMap<>();

        BufferedReader br;
        int maxTermSize = 0;

        /* Read collection */
        br = new BufferedReader(new InputStreamReader(new FileInputStream(Main.COLLECTION_PATH), StandardCharsets.UTF_8));

        String record = br.readLine();

        int i = 0;
        int maxTerms = 0;


        while(record != null) {
            ArrayList<String> preprocessed = TextProcessor.preprocessText(record);

            assertFalse(preprocessed.isEmpty());

            if(i < 3) //if isn't empty
            {
                System.out.println("Original record: " + record);
                System.out.println("Preprocessed record:" + preprocessed);
            }
            maxTerms = Math.max(preprocessed.size(), maxTerms);
            maxTermSize = findMaxWordLength(preprocessed);
            assertTrue(maxTermSize < 20);

            record = br.readLine();
            i++;
        }

        System.out.println("\n--------------------------------------------");
        System.out.println("Total number of records: " + i);
        System.out.println("Max terms per record: " + maxTerms);
        System.out.println("Longest term size: " + maxTermSize);
        System.out.println("--------------------------------------------");

    }


    @Test(timeout=100)
    public void xyzTesting() throws InterruptedException {
        System.out.println("Test xyzTesting");
//fallirà per timeout scaduto
        Thread.sleep(200);
        assertTrue(true);
    }

    @Test(expected=java.lang.Exception.class)
    public void nuovoTest() throws Exception {
        System.out.println("Test nuovoTest");
        assertTrue(true);
        throw new Exception();
    }

}