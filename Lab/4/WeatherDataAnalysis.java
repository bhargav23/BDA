import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce program to process weather data and find min/max temperatures per station.
 */
public class WeatherDataAnalysis {

    // Mapper Class
    public static class TemperatureMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text stationId = new Text();
        private DoubleWritable temperature = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length >= 3) {
                try {
                    stationId.set(fields[0]); // First column: Station ID
                    double temp = Double.parseDouble(fields[2]); // Third column: Temperature
                    temperature.set(temp);
                    context.write(stationId, temperature);
                } catch (NumberFormatException e) {
                    // Ignore malformed records
                }
            }
        }
    }

    // Reducer Class
    public static class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minTemp = Double.MAX_VALUE;
            double maxTemp = Double.MIN_VALUE;

            for (DoubleWritable value : values) {
                double temp = value.get();
                minTemp = Math.min(minTemp, temp);
                maxTemp = Math.max(maxTemp, temp);
            }

            String result = "Max Temp: " + maxTemp + "°C, Min Temp: " + minTemp + "°C";
            context.write(key, new Text(result));
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Analysis");

        job.setJarByClass(WeatherDataAnalysis.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
