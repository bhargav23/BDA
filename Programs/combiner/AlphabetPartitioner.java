import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }

        char firstLetter = key.toString().toLowerCase().charAt(0); // Get first letter
        int partition = firstLetter - 'a'; // Assign 0-25 based on 'a'-'z'

        if (partition < 0 || partition >= numPartitions) { 
            return 0; // Default to partition 0 if out of range
        }
        
        return partition;
    }
}
