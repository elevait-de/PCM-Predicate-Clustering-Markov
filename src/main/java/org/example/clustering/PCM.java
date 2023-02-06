package org.example.clustering;

import java.io.IOException;

public class PCM {
    public static void main(String[] args) throws IOException {

        cluster(PathConstants.PARTITION_OUTPUT_FOLDER, PathConstants.QUERIES_PATH, PathConstants.GRAPH_WEIGHT_FILE, PathConstants.PREDICATE_FILE, PathConstants.DATASET_PATH, PathConstants.TOTAL_PARTITIONS, PathConstants.CLUSTER_FILE);
//        clusterWithoutPartitioning(PathConstants.PARTITION_OUTPUT_FOLDER, PathConstants.QUERIES_PATH, PathConstants.GRAPH_WEIGHT_FILE, PathConstants.PREDICATE_FILE, PathConstants.DATASET_PATH, PathConstants.TOTAL_PARTITIONS, PathConstants.CLUSTER_FILE);

    }

    public static void cluster(String partitionOutputFolder, String queriesPath, String graphWeightFile, String predicateFile, String datasetPath, int totalPartitions, String clusterFile) throws IOException {

        // clean up should be done before starting the measurement
        PartitionGenerator.cleanExistingPartitionFiles(partitionOutputFolder);

        long start = System.currentTimeMillis();
        WeightGeneratorFromTestQueries.generateWeights(queriesPath, graphWeightFile, predicateFile);

        PartitionGenerator.getPredicateEncodings(predicateFile);
        PartitionGenerator.generatePartitionFiles(partitionOutputFolder, totalPartitions);

        PartitionGenerator.markovDistribution(partitionOutputFolder, datasetPath, queriesPath, totalPartitions, graphWeightFile, clusterFile, predicateFile);
        System.out.printf("PCM partitioning time (ms): %d \n", System.currentTimeMillis() - start);

    }

    public static void clusterWithoutPartitioning(String partitionOutputFolder, String queriesPath, String graphWeightFile, String predicateFile, int totalPartitions, String clusterFile) throws IOException {
        WeightGeneratorFromTestQueries.generateWeights(queriesPath, graphWeightFile, predicateFile);
        PartitionGenerator.getPredicateEncodings(predicateFile);
        MarkovClustering.findClusters(graphWeightFile, clusterFile);
//        PartitionGenerator.generatePartitionFiles(partitionOutputFolder, totalPartitions);
    }
}
