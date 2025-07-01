import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import utils.MyKafkaUtil;

import java.util.concurrent.TimeUnit;

public class bbbb {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 从Kafka读取数据
        DataStreamSource<String> ds1 = env.addSource(MyKafkaUtil.getKafkaConsumer("test_topic"));
        ds1.print("Kafka Data:");

        // 3. 配置HDFS sink（核心部分）

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://cdh01:8020//weblog/data"),
                        (String element, java.io.OutputStream stream) -> {
                            stream.write((element + "\n").getBytes());
                        })
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))  // 1分钟切分，方便测试
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10)) // 10秒无写入切分
                                .withMaxPartSize(1024 * 1024 * 16)                   // 10MB切分
                                .build())
                .withBucketCheckInterval(1000) // 每秒检查一次是否切分
                .build();

        // 4. 写入HDFS
        ds1.addSink(sink);

        // 5. 执行任务
        env.execute("Kafka to HDFS Job");
    }
}
