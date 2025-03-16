import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterWritable implements Writable {
    private BloomFilter bloomFilter;

    public BloomFilterWritable() {
        this.bloomFilter = new BloomFilter(1024, 5, Hash.MURMUR_HASH);
    }

    public BloomFilterWritable(BloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public void setBloomFilter(BloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        bloomFilter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        bloomFilter.readFields(in);
    }

    @Override
    public String toString() {
        return bloomFilter.toString(); // Converts Bloom filter to human-readable format
    }
}

