package advanced.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TemperatureWindReducer
        extends Reducer<Text, TemperatureWindWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<TemperatureWindWritable> values, Context context)
            throws IOException, InterruptedException {

        double somaTemp = 0;
        double somaVento = 0;
        int total = 0;

        for (TemperatureWindWritable val : values) {
            somaTemp += val.getTemperature();
            somaVento += val.getWindSpeed();
            total += val.getCount();
        }

        double mediaTemp = somaTemp / total;
        double mediaVento = somaVento / total;

        context.write(
                key,
                new Text(String.format("Temp: %.2f°C, Vento: %.2f km/h", mediaTemp, mediaVento))
        );
    }
}