import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import java.io.*;

public class SemiJoinMapper extends Mapper<Object, Text, Text, Text> {
    private BloomFilter bloomFilter = new BloomFilter();

    @Override
    protected void setup(Context context) throws IOException {
        try (DataInputStream input = new DataInputStream(new FileInputStream("vip_users.bloom"))) {
            bloomFilter.readFields(input);
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length < 2) return;

        String userID = fields[0];

        if (bloomFilter.membershipTest(new Key(userID.getBytes()))) {
            context.write(new Text(userID), value);
        }
    }
}
