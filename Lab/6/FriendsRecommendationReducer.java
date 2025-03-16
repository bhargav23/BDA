import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FriendsRecommendationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> friendCount = new HashMap<>();

        for (Text val : values) {
            String friend = val.toString();
            friendCount.put(friend, friendCount.getOrDefault(friend, 0) + 1);
        }

        String sortedRecommendations = friendCount.entrySet().stream()
            .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
            .map(entry -> entry.getKey() + "(" + entry.getValue() + ")")
            .collect(Collectors.joining(", "));

        context.write(key, new Text(sortedRecommendations));
    }
}
