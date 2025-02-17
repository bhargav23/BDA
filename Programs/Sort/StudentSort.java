import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StudentSort {

    public static class StudentMapper extends Mapper<Object, Text, Text, Text> {
        private Text studentName = new Text();
        private Text record = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(", ");
            if (fields.length == 3) {
                studentName.set(fields[1]);  // Student Name as key
                record.set(value);           // Entire record as value
                context.write(studentName, record);
            }
        }
    }

    public static class StudentReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> records = new ArrayList<>();
            for (Text val : values) {
                records.add(val.toString());
            }
            Collections.sort(records); // Sort records by natural order

            for (String record : records) {
                context.write(new Text(""), new Text(record)); // Output sorted records
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Sorting");

        job.setJarByClass(StudentSort.class);
        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
