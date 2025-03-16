import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FriendsRecommendationMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String[] users = parts[0].split(",");
        String[] mutualFriends = parts[1].split(",");

        for (String mutualFriend : mutualFriends) {
            context.write(new Text(users[0]), new Text(mutualFriend));
            context.write(new Text(users[1]), new Text(mutualFriend));
        }
    }
}
