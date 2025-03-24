package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureWindWritable implements Writable {
    private double temperature;
    private double windSpeed;
    private int count;

    public TemperatureWindWritable() {}

    public TemperatureWindWritable(double temperature, double windSpeed, int count) {
        this.temperature = temperature;
        this.windSpeed = windSpeed;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(temperature);
        out.writeDouble(windSpeed);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        temperature = in.readDouble();
        windSpeed = in.readDouble();
        count = in.readInt();
    }

    public double getTemperature() { return temperature; }
    public double getWindSpeed() { return windSpeed; }
    public int getCount() { return count; }
}