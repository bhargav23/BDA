import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final double DAMPING_FACTOR = 0.85;
    private static final double RANDOM_SURFER_PROB = 1 - DAMPING_FACTOR;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double newPageRank = 0.0;
        String outgoingLinks = "";

        for (Text val : values) {
            String value = val.toString();
            if (value.startsWith("|")) {
                outgoingLinks = value.substring(1);  // Preserve graph structure
            } else {
                newPageRank += Double.parseDouble(value);
            }
        }

        // Apply PageRank formula
        newPageRank = RANDOM_SURFER_PROB + DAMPING_FACTOR * newPageRank;

        context.write(key, new Text(newPageRank + "\t" + outgoingLinks));
    }
}
