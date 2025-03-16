import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendsOfFriendsDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FriendsOfFriendsDriver <input> <output1> <output2>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // First Job: Find Mutual Friends
        Job job1 = Job.getInstance(conf, "Mutual Friends Finder");
        job1.setJarByClass(FriendsOfFriendsDriver.class);
        job1.setMapperClass(MutualFriendsMapper.class);
        job1.setReducerClass(MutualFriendsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Second Job: Rank Friend Recommendations
        Job job2 = Job.getInstance(conf, "Friend Recommendation Ranking");
        job2.setJarByClass(FriendsOfFriendsDriver.class);
        job2.setMapperClass(FriendsRecommendationMapper.class);
        job2.setReducerClass(FriendsRecommendationReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
