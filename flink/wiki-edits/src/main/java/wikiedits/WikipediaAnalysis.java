package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.api.java.io.TextInputFormat;

public class WikipediaAnalysis {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

//    KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
//      .keyBy(new KeySelector<WikipediaEditEvent, String>() {
//        @Override
//        public String getKey(WikipediaEditEvent event) {
//          return event.getUser();
//        }
//      });
//
//    DataStream<Tuple2<String, Long>> result = keyedEdits
//      .timeWindow(Time.seconds(5))
//      .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
//        @Override
//        public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
//          acc.f0 = event.getUser();
//          acc.f1 += event.getByteDiff();
//          return acc;
//        }
//      });

      DataStream<WikiUserCount> result = edits
              .map(new MapFunction<WikipediaEditEvent, WikiUserCount>() {
                  @Override
                  public WikiUserCount map(WikipediaEditEvent e) throws Exception {
                      return new WikiUserCount(e.getUser(), e.getByteDiff());
                  }
              })
              .keyBy("user")
              .timeWindow(Time.seconds(5))
              .reduce( new ReduceFunction<WikiUserCount>() {
                  @Override
                  public WikiUserCount reduce(WikiUserCount a, WikiUserCount b) {
                      return new WikiUserCount(a.user, a.count + b.count);
                  }
              });

    result.print();

    see.execute();
  }

    // Data type for words with count
    public static class WikiUserCount {

        public String user;
        public int count;

        public WikiUserCount() {}

        public WikiUserCount(String user, int count) {
            this.user = user;
            this.count = count;
        }

        @Override
        public String toString() {
            return user + " : " + count;
        }
    }
}