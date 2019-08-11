package sqldemo;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.api.java.io.TextInputFormat;

public class FlinkSQLDemo {


    private final static AscendingTimestampExtractor extractor = new AscendingTimestampExtractor<WikiUserCount>() {
        @Override
        public long extractAscendingTimestamp(WikiUserCount element) {
            return element.timestamp;
        }
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<WikiUserCount> dataset = edits
                .map(new MapFunction<WikipediaEditEvent, WikiUserCount>() {
                    @Override
                    public WikiUserCount map(WikipediaEditEvent e) throws Exception {
                        return new WikiUserCount(e.getUser(), e.getByteDiff(), e.getTimestamp());
                    }
                }).assignTimestampsAndWatermarks(extractor);

        // Register it so we can use it in SQL
        tableEnv.registerDataStream("sensors", dataset, "user, count, rowtime.rowtime");

        String query = "SELECT room, TUMBLE_END(rowtime, INTERVAL '10' SECOND), AVG(temperature) AS avgTemp FROM sensors GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), room";
        Table table = tableEnv.sql(query);

        // Just for printing purposes, in reality you would need something other than Row
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }

    // Data type for words with count
    public static class WikiUserCount {

        public String user;
        public int count;
        public long timestamp;

        public WikiUserCount() {}

        public WikiUserCount(String user, int count, long timestamp) {
            this.user = user;
            this.count = count;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return user + " : " + count;
        }
    }
}