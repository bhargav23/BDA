import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BloomFilterMR <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bloom Filter MapReduce");
        job.setJarByClass(Main.class);

        job.setMapperClass(BloomFilterMR.class);
        job.setReducerClass(BloomReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BloomFilterWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BloomFilterWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);

        // Remove the output directory if it exists
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

