import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordMapper extends Mapper<Object, Text, Text, Text> {
    private String searchKeyword;

    @Override
    protected void setup(Context context) {
        searchKeyword = context.getConfiguration().get("searchKeyword").toLowerCase();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.toLowerCase().contains(searchKeyword)) {
            context.write(new Text("Matching Row"), new Text(line));  // Emit the matched row
        }
    }
}

