package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.utils.FileSystem.file_cleaner;
import static it.unipi.dii.aide.mircv.utils.Constants.*;


public class Main {

    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);
        long startTime, endTime;

        while (true) {

            System.out.println(ANSI_CYAN + "\n********** SEARCH ENGINE **********" + ANSI_RESET);
            System.out.println(ANSI_CYAN +
                    "\n\tSelect an option:" +
                    "\n\t  m -> try merge only" +
                    "\n\t  i -> build the index" +
                    "\n\t  l -> load from disk" +
                    "\n\t  q -> query mode" +
                    "\n\t  x -> exit"
                    + ANSI_RESET);
            System.out.println(ANSI_CYAN + "\n***********************************\n" + ANSI_RESET);
            String mode = sc.nextLine();


            switch (mode) {

                case "m":       // per debugging, prova solo il merge
                    DataStructureHandler.readBlockOffsetsFromDisk();

                    startTime = System.currentTimeMillis();
                    IndexMerger.mergeBlocks();
                    endTime = System.currentTimeMillis();           // end time to read Document Index from disk
                    System.out.println(ANSI_YELLOW + "Merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

                    continue;

                case "i":
                    file_cleaner();                             // delete all created files

                    Flag.setSws(getUserChoice(sc, "stopwords removal"));
                    Flag.setCompression(getUserChoice(sc, "compression"));
                    Flag.setScoring(getUserChoice(sc, "scoring"));

                    DataStructureHandler.storeFlagsIntoDisk();

                    // Do SPIMI Algorithm
                    System.out.println("\nIndexing...");
                    startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
                    DataStructureHandler.SPIMIalgorithm();
                    endTime = System.currentTimeMillis();           // end time of SPIMI algorithm
                    System.out.println(ANSI_YELLOW + "\nSPIMI Algorithm done in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

                    // merge blocks into disk
                    startTime = System.currentTimeMillis();         // start time to merge blocks
                    IndexMerger.mergeBlocks();
                    endTime = System.currentTimeMillis();           // end time of merge blocks
                    System.out.println(ANSI_YELLOW + "\nBlocks merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

                    continue;

                case "l":

                    // Read Flags from disk
                    DataStructureHandler.readFlagsFromDisk();

                    //Read Document Table from disk and put into memory
                    startTime = System.currentTimeMillis();
                    DataStructureHandler.readDocumentTableFromDisk();
                    endTime = System.currentTimeMillis();           // end time to read Document Index from disk
                    System.out.println(ANSI_YELLOW + "Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

                    //Read Dictionary from disk
                    startTime = System.currentTimeMillis();         // start time to read Dictionary from disk
                    DataStructureHandler.readDictionaryFromDisk();
                    endTime = System.currentTimeMillis();           // end time to read Dictionary from disk
                    System.out.println(ANSI_YELLOW + "Dictionary loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                    continue;

                case "q":

                    System.out.print(ANSI_CYAN + "Insert query: \n" + ANSI_RESET);
                    String query = sc.nextLine();

                    if (query == null || query.isEmpty()) {
                        System.out.println(ANSI_CYAN + "Error: the query is empty. Retry" + ANSI_RESET);
                        continue;
                    }

                    int validN = 0;     // 1 = positive number - 0 = negative number or not a number
                    do {
                        System.out.println(ANSI_CYAN + "Type the number of results to retrieve (10 or 20)" + ANSI_RESET);
                        try {
                            int n = Integer.parseInt(sc.nextLine());
                            validN = (n > 0) ? 1 : 0;
                        } catch (NumberFormatException nfe) {
                            System.out.println(ANSI_CYAN + "Insert a valid positive number" + ANSI_RESET);
                        }
                    } while (validN == 0);
                    continue;

                default:
                    return;
            }
        }

    }


    /**
     * fucntion to get the choise of the user for options, the options are pass
     *
     * @param sc     scanner to get the choice of the user inserted via keyboard
     * @param option options passed by parameter
     * @return true if the user chooses yes (enter Y), false if the user chooses no (enter N)
     */
    private static boolean getUserChoice(Scanner sc, String option) {
        while (true) {
            System.out.println(ANSI_CYAN + "\nType Y or N for " + option + " options" + ANSI_RESET);   // print of the option
            String choice = sc.nextLine().toUpperCase();                    // take the user's choice
            // check the user's input
            if (choice.equals("Y")) {
                return true;
            } else if (choice.equals("N")) {
                return false;
            }
        }
    }

}


