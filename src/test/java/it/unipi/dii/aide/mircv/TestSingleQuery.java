package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.utils.TextProcessor;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.utils.Constants.COLLECTION_PATH;

public class TestSingleQuery {

    public static void main(String[] args) {

        File file = new File(COLLECTION_PATH);
        try (
                final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)))
        ) {

            // read from compressed collection
            TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
            BufferedReader buffer_collection;
            if (tarArchiveEntry == null)
                return;
            buffer_collection = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));

            String record;

            while ((record = buffer_collection.readLine()) != null) {

                String query = "who is aziz hashim";
                ArrayList<String> prep_query = TextProcessor.preprocessText(query);

                boolean[] check = new boolean[prep_query.size()];

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

                // scan all term in the current document
                for (String term : preprocessed) {
                    for (int i = 0; i < prep_query.size(); i++)
                        if(term.equals(prep_query.get(i)))
                            check[i] = true;
                }
                boolean ok = true;
                for (boolean c : check)
                    ok &= c;

                if(ok)
                    System.out.println(record);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
