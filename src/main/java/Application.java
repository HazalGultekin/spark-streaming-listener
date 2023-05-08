import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;

public class Application {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","E:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().appName("SparkStreamingMessageListener");

        Dataset<Row> rawData = sparkSession.readStream().format("socket").option("host","localhost");

        Dataset<String> data = rawData.as(Encoders.STRING());

        Dataset<String> stringDataset = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        Dataset<Row> groupedData = stringDataset.groupBy("value").count();

        StreamingQuery start = groupedData.writeStream().outputMode("complete").format("console").start();

        start.awaitTermination();
    }
}
