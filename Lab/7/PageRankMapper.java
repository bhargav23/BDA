import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 3) return;

        String node = parts[0];
        double pageRank = Double.parseDouble(parts[1]);
        String[] outgoingLinks = parts[2].split(",");

        for (String link : outgoingLinks) {
            double sharedPageRank = pageRank / outgoingLinks.length;
            context.write(new Text(link), new Text(String.valueOf(sharedPageRank)));
        }

        // Emit original structure to preserve graph topology
        context.write(new Text(node), new Text("|" + parts[2]));
    }
}
