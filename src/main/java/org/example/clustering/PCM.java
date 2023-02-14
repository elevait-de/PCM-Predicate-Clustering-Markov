package org.example.clustering;

import java.io.IOException;
import java.nio.file.Paths;

public class PCM {
    public static void main(String[] args) throws IOException {

        Measurement measurement = new Measurement(PathConstants.MEASUREMENT_PATH);
        measurement.setQuerySetPath(Paths.get(PathConstants.QUERIES_PATH).toAbsolutePath().toString());
        measurement.setDataSetPath(Paths.get(PathConstants.DATASET_PATH).toAbsolutePath().toString());

        cluster(PathConstants.PARTITION_OUTPUT_FOLDER, PathConstants.QUERIES_PATH, PathConstants.GRAPH_WEIGHT_FILE, PathConstants.PREDICATE_FILE, PathConstants.DATASET_PATH, PathConstants.TOTAL_PARTITIONS, PathConstants.CLUSTER_FILE, measurement);

        measurement.setPartitionFileSizes();
        measurement.writeToFile(false);

    }

    public static void cluster(String partitionOutputFolder, String queriesPath, String graphWeightFile, String predicateFile, String datasetPath, int totalPartitions, String clusterFile, Measurement measurement) throws IOException {

        // clean up should be done before starting the measurement
        PartitionGenerator.cleanExistingPartitionFiles(partitionOutputFolder);

        long start = System.currentTimeMillis();
        WeightGeneratorFromTestQueries.generateWeights(queriesPath, graphWeightFile, predicateFile);

        PartitionGenerator.getPredicateEncodings(predicateFile);
        PartitionGenerator.generatePartitionFiles(partitionOutputFolder, totalPartitions);

        PartitionGenerator.markovDistribution(partitionOutputFolder, datasetPath, queriesPath, totalPartitions, graphWeightFile, clusterFile, predicateFile);
        measurement.setPartitionTime(System.currentTimeMillis() - start);

    }

    public static void clusterWithoutPartitioning(String partitionOutputFolder, String queriesPath, String graphWeightFile, String predicateFile, int totalPartitions, String clusterFile) throws IOException {
        WeightGeneratorFromTestQueries.generateWeights(queriesPath, graphWeightFile, predicateFile);
        PartitionGenerator.getPredicateEncodings(predicateFile);
        MarkovClustering.findClusters(graphWeightFile, clusterFile);
    }
}
