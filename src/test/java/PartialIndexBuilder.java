import it.unipi.dii.aide.mircv.utils.Constants;
import org.junit.jupiter.api.Test;

public class PartialIndexBuilder {
    @Test
    void testSPIMI() {
/*
        String outputTarGzFileName = "src/main/resources/small_collection.tar.gz";
        String tsvFileName = "src/main/resources/small_collection.tsv";
        try (
                FileOutputStream fos = new FileOutputStream(outputTarGzFileName);
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                GzipCompressorOutputStream gzOut = new GzipCompressorOutputStream(bos);
                TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzOut);
        ) {
            File tsvFile = new File(tsvFileName);
            TarArchiveEntry entry = new TarArchiveEntry(tsvFile, tsvFile.getName());
            tarOut.putArchiveEntry(entry);

            try (BufferedInputStream tsvIn = new BufferedInputStream(new FileInputStream(tsvFile))) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = tsvIn.read(buffer)) != -1) {
                    tarOut.write(buffer, 0, len);
                }
            }

            tarOut.closeArchiveEntry();
        } catch (IOException e) {
            e.printStackTrace();
        }
*/


        Constants.COLLECTION_PATH = "src/main/resources/small_collection.tar.gz";
        it.unipi.dii.aide.mircv.index.PartialIndexBuilder.SPIMI();
    }

/*    @Test
    void name() {
        String inputFile = "src/main/resources/collection.tsv";
        String outputFile = "src/main/resources/small_collection.tsv";
        int numberOfLinesToRead = 1000;

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String line;
        int lineCount = 0;

        // Leggi e scrivi le prime 1000 righe
        while (true) {
            try {
                if (!((line = reader.readLine()) != null && lineCount < numberOfLinesToRead)) break;
                System.out.println(line);
                writer.write(line);
                writer.newLine();
                lineCount++;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        try {
            reader.close();
            writer.close();
            System.out.println("Le prime " + lineCount + " righe sono state scritte in " + outputFile);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }*/



    
}
