import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PageRankDriver <input> <output> <iterations>");
            System.exit(2);
        }

        int iterations = Integer.parseInt(args[2]);
        String inputPath = args[0];
        String outputPath = args[1];

        for (int i = 0; i < iterations; i++) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "PageRank Iteration " + i);
            job.setJarByClass(PageRankDriver.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            String input = (i == 0) ? inputPath : outputPath + (i - 1);
            String output = outputPath + i;

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }
}
