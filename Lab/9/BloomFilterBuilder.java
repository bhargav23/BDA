import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import java.io.*;

public class BloomFilterBuilder {
    private static final int NUM_ELEMENTS = 10000;
    private static final float FALSE_POSITIVE_RATE = 0.01f;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: BloomFilterBuilder <input_file> <output_file>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];

        int vectorSize = (int) (-NUM_ELEMENTS * Math.log(FALSE_POSITIVE_RATE) / (Math.log(2) * Math.log(2)));
        int numHashFunctions = (int) (vectorSize / NUM_ELEMENTS * Math.log(2));

        BloomFilter bloomFilter = new BloomFilter(vectorSize, numHashFunctions, Hash.MURMUR_HASH);

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                bloomFilter.add(new Key(line.trim().getBytes()));
            }
        }

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(outputFile))) {
            bloomFilter.write(out);
        }

        System.out.println("Bloom filter saved to " + outputFile);
    }
}
