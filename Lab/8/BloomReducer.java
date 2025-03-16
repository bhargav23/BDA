import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;
import java.io.IOException;

public class BloomReducer extends Reducer<Text, BloomFilterWritable, Text, BloomFilterWritable> {
    private BloomFilterWritable resultFilter = new BloomFilterWritable();

    @Override
    protected void reduce(Text key, Iterable<BloomFilterWritable> values, Context context) 
            throws IOException, InterruptedException {
        BloomFilter combinedFilter = new BloomFilter(1024, 5, org.apache.hadoop.util.hash.Hash.MURMUR_HASH);

        for (BloomFilterWritable bfWritable : values) {
            combinedFilter.or(bfWritable.getBloomFilter()); // Merge Bloom filters
        }

        resultFilter.setBloomFilter(combinedFilter);
        context.write(key, resultFilter);
    }
}

