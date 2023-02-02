import java.io.IOException;

public class PCM {
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        WeightGeneratorFromTestQueries.generateWeights();

        PartitionGenerator.getPredicateEncodings();
        PartitionGenerator.generatePartitionFiles();

        PartitionGenerator.markovDistribution();
        System.out.printf("PCM finished after: %d ms\n", System.currentTimeMillis() - start);

    }
}
