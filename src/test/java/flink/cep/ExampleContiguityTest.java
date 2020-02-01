package flink.cep;

import example.flink.cep.Event;
import example.flink.cep.MonitoringEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ExampleContiguityTest {

    //Последовательность источника данных рандомная
    @Test
    public void checkStrictContiguity() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Event> inputEventStream = env.addSource(new MonitoringEvent())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<Event, ?> warningPattern = Pattern.<Event>begin("first").where(new IterativeCondition<Event>() {
            private static final long serialVersionUID = 2392863109523984059L;

            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                return value.getId() == 1;
            }
        }).next("next").where(new IterativeCondition<Event>() {
            private static final long serialVersionUID = 2392863109523984059L;

            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                return value.getId() == 2;
            }
        });

        PatternStream<Event> idEventStream = CEP.pattern(inputEventStream, warningPattern);
        DataStream<Map<String, List<Event>>> warnings = idEventStream.select(new PatternSelectFunction<Event, Map<String, List<Event>>>() {
            @Override
            public Map<String, List<Event>> select(Map<String, List<Event>> pattern) throws Exception {
                return pattern;
            }
        });
        warnings.print();
        env.execute("monitoring job");
    }

    @Test
    public void checkRelaxedContiguity() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> inputEventStream = env.addSource(new MonitoringEvent())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<Event, ?> warningPattern = Pattern.<Event>begin("first").where(new IterativeCondition<Event>() {
            private static final long serialVersionUID = 2392863109523984059L;

            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                return value.getId() == 1;
            }
        }).followedBy("followedBy").where(new IterativeCondition<Event>() {
            private static final long serialVersionUID = 2392863109523984059L;

            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                return value.getId() == 2;
            }
        });

        PatternStream<Event> idEventStream = CEP.pattern(inputEventStream, warningPattern);

        DataStream<Map<String, List<Event>>> warnings = idEventStream.select(new PatternSelectFunction<Event, Map<String, List<Event>>>() {
            @Override
            public Map<String, List<Event>> select(Map<String, List<Event>> pattern) throws Exception {
                return pattern;
            }
        });
        warnings.print();
        env.execute("monitoring job");
    }

    @Test
    public void checkNonDeterministicRelaxedContiguity() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> inputEventStream = env.addSource(new MonitoringEvent())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<Event, ?> warningPattern = Pattern.<Event>begin("first").where(new IterativeCondition<Event>() {
            private static final long serialVersionUID = 2392863109523984059L;

            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                return value.getId() == 1;
            }
        }).followedByAny("followedByAny").where(new IterativeCondition<Event>() {
            private static final long serialVersionUID = 2392863109523984059L;

            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                return value.getId() == 2;
            }
        });

        PatternStream<Event> idEventStream = CEP.pattern(inputEventStream, warningPattern);

        DataStream<Map<String, List<Event>>> warnings = idEventStream.select(new PatternSelectFunction<Event, Map<String, List<Event>>>() {
            @Override
            public Map<String, List<Event>> select(Map<String, List<Event>> pattern) throws Exception {
                return pattern;
            }
        });

        warnings.print();
        env.execute("monitoring job");
    }
}
