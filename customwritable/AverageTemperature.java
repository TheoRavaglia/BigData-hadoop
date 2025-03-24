package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AverageTemperature {

    // Custom Writable para armazenar soma e contagem
    public static class FireAvgTempWritable implements Writable {
        private int n;
        private float soma;

        public FireAvgTempWritable() {}

        public FireAvgTempWritable(int n, float soma) {
            this.n = n;
            this.soma = soma;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(n);
            out.writeFloat(soma);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            n = in.readInt();
            soma = in.readFloat();
        }

        public int getN() { return n; }
        public float getSoma() { return soma; }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (files.length < 2) {
            System.err.println("Uso: AverageTemperature <input path> <output path>");
            System.err.println("Exemplo: AverageTemperature /input /output");
            System.exit(1);
        }

        // Configurações para execução local
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "media-temperatura-mensal");
        job.setJarByClass(AverageTemperature.class);
        job.setMapperClass(MapForAverage.class);
        job.setCombinerClass(CombineForAverage.class);
        job.setReducerClass(ReduceForAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FireAvgTempWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(files[0]));
        FileOutputFormat.setOutputPath(job, new Path(files[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        private static final Map<String, String> MESES = new HashMap<>();

        static {
            // Mapeamento de meses para números
            MESES.put("jan", "01");
            MESES.put("feb", "02");
            MESES.put("mar", "03");
            MESES.put("apr", "04");
            MESES.put("may", "05");
            MESES.put("jun", "06");
            MESES.put("jul", "07");
            MESES.put("aug", "08");
            MESES.put("sep", "09");
            MESES.put("oct", "10");
            MESES.put("nov", "11");
            MESES.put("dec", "12");
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] colunas = value.toString().split(",");

            if (colunas.length >= 9) {
                try {
                    // Extrai o mês (ex: "mar") e converte para número (ex: "03")
                    String mesAbreviado = colunas[2].toLowerCase();
                    String mesNumero = MESES.getOrDefault(mesAbreviado, "00");

                    // Extrai a temperatura da coluna 8 (índice 7)
                    float temperatura = Float.parseFloat(colunas[7]);

                    // Emite (mês, (1, temperatura))
                    context.write(
                            new Text(mesNumero),
                            new FireAvgTempWritable(1, temperatura)
                    );
                } catch (Exception e) {
                    context.getCounter("Erros", "Registros Invalidos").increment(1);
                }
            }
        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context context)
                throws IOException, InterruptedException {

            int totalContagem = 0;
            float totalSoma = 0;

            for (FireAvgTempWritable val : values) {
                totalContagem += val.getN();
                totalSoma += val.getSoma();
            }

            context.write(key, new FireAvgTempWritable(totalContagem, totalSoma));
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context context)
                throws IOException, InterruptedException {

            int contagemTotal = 0;
            float somaTotal = 0;

            for (FireAvgTempWritable val : values) {
                contagemTotal += val.getN();
                somaTotal += val.getSoma();
            }

            float media = somaTotal / contagemTotal;
            context.write(key, new FloatWritable(media));
        }
    }
}