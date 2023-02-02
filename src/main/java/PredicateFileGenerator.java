

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.UUID;

public class PredicateFileGenerator {
    public static void main(String[] args) {


        System.out.println("Finding test Pattterns ...");

        generatePredicateFiles(PathConstants.DATASET_PATH, PathConstants.PREDICATE_FILES);

        System.out.println("Terminated Successfully");

    }

    static public void generatePredicateFiles(String datasetFile, String outputFolder) {
        HashMap<String, BufferedWriter> predicateToFile = new HashMap<>();
        if (!outputFolder.endsWith("/")) {
            outputFolder = outputFolder + "/";
        }
        try (BufferedReader datasetReader = new BufferedReader(new FileReader(datasetFile))) {
            String line;
            while ((line = datasetReader.readLine()) != null) {
                String predicate = PartitionGenerator.getPredicate(line);

                if (!predicateToFile.containsKey(predicate)) {
                    BufferedWriter writer = Files.newBufferedWriter(Path.of(outputFolder + UUID.randomUUID()));
                    predicateToFile.put(predicate, writer);
                }

                BufferedWriter writer = predicateToFile.get(predicate);
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Error reading the dataset with message: " + e.getMessage());
            e.printStackTrace();
        } finally {
            predicateToFile.values().forEach(bufferedWriter -> {
                try {
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    System.out.println("Error closing the predicate file writers with message: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }

    }

}
