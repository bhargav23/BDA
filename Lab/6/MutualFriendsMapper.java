import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

public class MutualFriendsMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String user = parts[0];
        String[] friends = parts[1].split(",");

        for (String friend : friends) {
            String userPair = user.compareTo(friend) < 0 ? user + "," + friend : friend + "," + user;
            context.write(new Text(userPair), new Text(parts[1]));
        }
    }
}
