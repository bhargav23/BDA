import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterMapReduce {

    // Mapper class
    public static class BloomFilterMapper extends Mapper<Object, Text, Text, BloomFilterWritable> {
        private BloomFilter bloomFilter;
        
        @Override
        protected void setup(Context context) {
            int vectorSize = 10000;  // Adjust size as needed
            int numHash = 5;  // Number of hash functions
            bloomFilter = new BloomFilter(vectorSize, numHash, Hash.MURMUR_HASH);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            bloomFilter.add(new Key(value.toString().getBytes()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("bloom"), new BloomFilterWritable(bloomFilter));
        }
    }

    // Reducer class
    public static class BloomFilterReducer extends Reducer<Text, BloomFilterWritable, Text, BloomFilterWritable> {
        private BloomFilter finalBloomFilter;

        @Override
        protected void setup(Context context) {
            int vectorSize = 10000;
            int numHash = 5;
            finalBloomFilter = new BloomFilter(vectorSize, numHash, Hash.MURMUR_HASH);
        }

        @Override
        public void reduce(Text key, Iterable<BloomFilterWritable> values, Context context) throws IOException, InterruptedException {
            for (BloomFilterWritable value : values) {
                finalBloomFilter.or(value.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("final_bloom_filter"), new BloomFilterWritable(finalBloomFilter));
        }
    }

    // Writable wrapper for BloomFilter
    public static class BloomFilterWritable implements Writable {
        private BloomFilter bloomFilter;

        public BloomFilterWritable() {
            this.bloomFilter = new BloomFilter(10000, 5, Hash.MURMUR_HASH);
        }

        public BloomFilterWritable(BloomFilter bloomFilter) {
            this.bloomFilter = bloomFilter;
        }

        public BloomFilter get() {
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
    }

    // Driver class
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(BloomFilterMapReduce.class);
        job.setJobName("Bloom Filter MapReduce");
        
        job.setMapperClass(BloomFilterMapper.class);
        job.setReducerClass(BloomFilterReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BloomFilterWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BloomFilterWritable.class);
        
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[0]));
        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
