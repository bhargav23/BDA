import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class SemiJoinBloomFilter {

    public static class BloomFilterMapper extends Mapper<Object, Text, Text, Text> {
        private BloomFilter bloomFilter = new BloomFilter(10000, 5, Hash.MURMUR_HASH);

        @Override
        protected void setup(Context context) throws IOException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (files != null && files.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
                String line;
                while ((line = reader.readLine()) != null) {
                    bloomFilter.add(new Key(line.getBytes()));
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length > 0 && bloomFilter.membershipTest(new Key(fields[0].getBytes()))) {
                context.write(new Text(fields[0]), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Semi-Join Bloom Filter");
        job.setJarByClass(SemiJoinBloomFilter.class);
        
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        
        job.setMapperClass(BloomFilterMapper.class);
        job.setNumReduceTasks(0); // No reducer needed
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
