package org.example.clustering;

import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Measurement {

    private String dataSetPath;
    private long dataSetSize;
    private String querySetPath;
    private long querySetSize;
    private long partitionTime;
    private List<Long> partitionFileSizes;
    private final String measurementFilePath;

    public Measurement(String measurementFilePath) {
        this.measurementFilePath = measurementFilePath;
    }

    public void writeToFile(boolean addHeader) {
        try (CSVWriter writer = new CSVWriter(new BufferedWriter(new FileWriter(measurementFilePath, true)), ',', '"', '\\', "\n")) {
            List<String[]> list = new ArrayList<>();
            if (addHeader && !new File(measurementFilePath).exists()) {
                list.add(generateHeader());
            }

            List<String> measurement = new ArrayList<>();

            measurement.add(dataSetPath);
            measurement.add(String.valueOf(dataSetSize));
            measurement.add(querySetPath);
            measurement.add(String.valueOf(querySetSize));
            measurement.add(String.valueOf(partitionTime));
            measurement.add(String.valueOf(partitionFileSizes.size()));
            partitionFileSizes.forEach(fileSize -> measurement.add(String.valueOf(fileSize)));

            list.add(measurement.toArray(String[]::new));
            writer.writeAll(list, false);
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String[] generateHeader() {
        List<String> header = new ArrayList<>();
        header.add("dataset path");
        header.add("dataset size");
        header.add("query set path");
        header.add("query set size");
        header.add("partition time");
        header.add("number of partitions");
        IntStream.range(0, partitionFileSizes.size()).forEach(i -> header.add("size of partition " + i));
        return header.toArray(String[]::new);
    }

    public Measurement setPartitionTime(long partitionTime) {
        this.partitionTime = partitionTime;
        return this;
    }

    public Measurement setPartitionFileSizes() throws IOException {
        partitionFileSizes = new ArrayList<>();
        for (int i = 0; i < PathConstants.TOTAL_PARTITIONS; i++) {
            try (Stream<String> lines = Files.lines(Paths.get(PathConstants.PARTITION_OUTPUT_FOLDER + "/Partition" + i))) {
                long numberOfLines = lines.count();
                partitionFileSizes.add(numberOfLines);
            }
        }
        return this;
    }

    public Measurement setQuerySetPath(String querySetPath) {
        this.querySetPath = querySetPath;
        setQuerySetSize();
        return this;
    }

    private void setQuerySetSize() {
        try (Stream<String> lines = Files.lines(Paths.get(querySetPath))) {
            querySetSize = lines.count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Measurement setDataSetPath(String dataSetPath) {
        this.dataSetPath = dataSetPath;
        setDataSetSize();
        return this;
    }

    private void setDataSetSize() {
        try (Stream<String> lines = Files.lines(Paths.get(this.dataSetPath))) {
            this.dataSetSize = lines.count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
