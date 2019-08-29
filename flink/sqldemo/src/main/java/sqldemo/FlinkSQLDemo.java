package sqldemo;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.api.java.io.TextInputFormat;

import java.sql.Timestamp;

public class FlinkSQLDemo {


    private final static AscendingTimestampExtractor extractor = new AscendingTimestampExtractor<WikiUserCount>() {
        @Override
        public long extractAscendingTimestamp(WikiUserCount element) {
            return element.timestamp;
        }
    };

    public static void main(String[] args) throws Exception {

        //StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());


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
        tableEnv.registerDataStream("sensors", dataset, "user, wordcount, timestamp, proctime.proctime");

        String query = "SELECT user, SUM(wordcount) AS total,  TUMBLE_END(proctime, INTERVAL '10' SECOND) FROM sensors GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), user";
        Table table = tableEnv.sqlQuery(query); // https://flink.sojb.cn/dev/table/sql.html 1.7 中修改为 .sqlQuery

        // convert to datastream https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/table/common.html#integration-with-datastream-and-dataset-api

        TupleTypeInfo<Tuple3<String, Integer, Timestamp>> tupleType = new TupleTypeInfo<>(
                Types.STRING(),
                Types.INT(),
                Types.SQL_TIMESTAMP());
        DataStream<Tuple3<String, Integer, Timestamp>> dsTuple =
                tableEnv.toAppendStream(table, tupleType);

        dsTuple.print();
        // Just for printing purposes, in reality you would need something other than Row
        //DataStream<WikiUserCount> stream = tableEnv.toAppendStream(table, WikiUserCount.class);

        //stream.print();
        System.out.println(env.getExecutionPlan());
        env.execute("print job");
    }

    // Data type for words with count
    public static class WikiUserCount {

        public String user;
        public int wordcount;
        public long timestamp;

        public WikiUserCount() {}

        public WikiUserCount(String user, int count, long timestamp) {
            this.user = user;
            this.wordcount = count;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return user + " : " + wordcount;
        }
    }
}