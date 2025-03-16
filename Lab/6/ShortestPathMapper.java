import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        String node = parts[0];
        int distance = parts[1].equals("INF") ? Integer.MAX_VALUE : Integer.parseInt(parts[1]);
        String previousNode = parts[2];
        String[] neighbors = parts.length > 3 ? parts[3].split(",") : new String[0];

        // Emit the node itself (to retain structure)
        context.write(new Text(node), new Text(parts[1] + "\t" + previousNode + "\t" + parts[3]));

        // Propagate distance to neighbors
        if (distance != Integer.MAX_VALUE) {
            for (String neighbor : neighbors) {
                context.write(new Text(neighbor), new Text((distance + 1) + "\t" + node));
            }
        }
    }
}
