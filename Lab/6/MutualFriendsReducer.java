import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MutualFriendsReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> mutualFriends = new HashSet<>();
        boolean firstList = true;

        for (Text val : values) {
            Set<String> friendsSet = new HashSet<>(Arrays.asList(val.toString().split(",")));

            if (firstList) {
                mutualFriends.addAll(friendsSet);
                firstList = false;
            } else {
                mutualFriends.retainAll(friendsSet);
            }
        }

        if (!mutualFriends.isEmpty()) {
            context.write(key, new Text(String.join(",", mutualFriends)));
        }
    }
}
