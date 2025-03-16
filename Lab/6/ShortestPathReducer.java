import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ShortestPathReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String neighbors = "";
        String previousNode = "NONE";
        int minDistance = Integer.MAX_VALUE;

        for (Text val : values) {
            String[] parts = val.toString().split("\t");

            if (parts.length == 3) { // Original node format
                neighbors = parts[2];
            }

            int distance = parts[0].equals("INF") ? Integer.MAX_VALUE : Integer.parseInt(parts[0]);
            if (distance < minDistance) {
                minDistance = distance;
                previousNode = parts[1];
            }
        }

        context.write(key, new Text((minDistance == Integer.MAX_VALUE ? "INF" : minDistance) + "\t" + previousNode + "\t" + neighbors));
    }
}
