package advanced.customwritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

class TemperatureWindMapper
        extends Mapper<LongWritable, Text, Text, TemperatureWindWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] campos = value.toString().split(",");
        if (campos.length >= 3) {
            try {
                String mes = campos[0].substring(0, 7); // Formato "YYYY-MM"
                double temperatura = Double.parseDouble(campos[1]);
                double vento = Double.parseDouble(campos[2]);

                context.write(
                        new Text(mes),
                        new TemperatureWindWritable(temperatura, vento, 1)
                );
            } catch (Exception e) {
                // Ignora linhas inválidas
            }
        }
    }
}