package flink.cep;

import example.flink.cep.EventSkipStrategy;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExampleSkipStrategy {

    public class MonitoringEventSkipStrategySkipStrategy implements SourceFunction<EventSkipStrategy> {

        @Override
        public void run(SourceContext sourceContext) {

            List<EventSkipStrategy> list = Stream.of(
                    new EventSkipStrategy("a1"),
                    new EventSkipStrategy("a2"),
                    new EventSkipStrategy("b3"),
                    new EventSkipStrategy("a4"),
                    new EventSkipStrategy("b5"),
                    new EventSkipStrategy("b6"),
                    new EventSkipStrategy("a7")).collect(Collectors.toList());

            for (EventSkipStrategy EventSkipStrategy : list) {
                System.out.println("Source  " + EventSkipStrategy);
                sourceContext.collect(EventSkipStrategy);
            }
        }

        @Override
        public void cancel() {
        }
    }


    @Test
    public void checkNoSkip() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<EventSkipStrategy> inputEventSkipStrategyStream = env.addSource(new MonitoringEventSkipStrategySkipStrategy())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<EventSkipStrategy, ?> warningPattern = Pattern.<EventSkipStrategy>begin("first")
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStream = CEP.pattern(inputEventSkipStrategyStream, warningPattern);

        DataStream<Map<String, List<EventSkipStrategy>>> warnings = idEventSkipStrategyStream.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warnings.print();
        env.execute("monitoring job");
    }

    @Test
    public void checkSkipToNext() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<EventSkipStrategy> inputEventSkipStrategyStream = env.addSource(new MonitoringEventSkipStrategySkipStrategy())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<EventSkipStrategy, ?> warningPattern = Pattern.<EventSkipStrategy>begin("first", AfterMatchSkipStrategy.skipToNext())
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStream = CEP.pattern(inputEventSkipStrategyStream, warningPattern);

        DataStream<Map<String, List<EventSkipStrategy>>> warnings = idEventSkipStrategyStream.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warnings.print();
        env.execute("monitoring job");
    }

    @Test
    public void checkSkipPastLastEvent() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<EventSkipStrategy> inputEventSkipStrategyStream = env.addSource(new MonitoringEventSkipStrategySkipStrategy())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<EventSkipStrategy, ?> warningPattern = Pattern.<EventSkipStrategy>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStream = CEP.pattern(inputEventSkipStrategyStream, warningPattern);

        DataStream<Map<String, List<EventSkipStrategy>>> warnings = idEventSkipStrategyStream.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warnings.print();
        env.execute("monitoring job");
    }


    @Test
    public void checkSkipToFirst() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<EventSkipStrategy> inputEventSkipStrategyStream = env.addSource(new MonitoringEventSkipStrategySkipStrategy())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<EventSkipStrategy, ?> warningPatternFirst = Pattern.<EventSkipStrategy>begin("first", AfterMatchSkipStrategy.skipToFirst("first"))
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStream = CEP.pattern(inputEventSkipStrategyStream, warningPatternFirst);

        DataStream<Map<String, List<EventSkipStrategy>>> warnings = idEventSkipStrategyStream.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warnings.print();
        env.execute("monitoring job");

        Pattern<EventSkipStrategy, ?> warningPatternNext = Pattern.<EventSkipStrategy>begin("first", AfterMatchSkipStrategy.skipToFirst("next"))
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStreamNext = CEP.pattern(inputEventSkipStrategyStream, warningPatternNext);

        DataStream<Map<String, List<EventSkipStrategy>>> warningsNext = idEventSkipStrategyStreamNext.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warningsNext.print();
        env.execute("monitoring job");
    }

    public class MonitoringEventSkipStrategySkipToLast implements SourceFunction<EventSkipStrategy> {

        @Override
        public void run(SourceContext sourceContext) {

            List<EventSkipStrategy> list = Stream.of(
                    new EventSkipStrategy("ab1"),
                    new EventSkipStrategy("ab2"),
                    new EventSkipStrategy("a3"),
                    new EventSkipStrategy("ab4"),
                    new EventSkipStrategy("ab5"),
                    new EventSkipStrategy("b6"),
                    new EventSkipStrategy("ab7")).collect(Collectors.toList());

            for (EventSkipStrategy EventSkipStrategy : list) {
                System.out.println("Source  " + EventSkipStrategy);
                sourceContext.collect(EventSkipStrategy);
            }
        }

        @Override
        public void cancel() {
        }
    }


    @Test
    public void checkSkipToLast() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<EventSkipStrategy> inputEventSkipStrategyStream = env.addSource(new MonitoringEventSkipStrategySkipToLast())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        Pattern<EventSkipStrategy, ?> warningPatternFirst = Pattern.<EventSkipStrategy>begin("first", AfterMatchSkipStrategy.skipToLast("first"))
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStream = CEP.pattern(inputEventSkipStrategyStream, warningPatternFirst);

        DataStream<Map<String, List<EventSkipStrategy>>> warnings = idEventSkipStrategyStream.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warnings.print();
        env.execute("monitoring job");

        Pattern<EventSkipStrategy, ?> warningPatternNext = Pattern.<EventSkipStrategy>begin("first", AfterMatchSkipStrategy.skipToLast("next"))
                .where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("a");
                    }
                }).times(2).next("next").where(new IterativeCondition<EventSkipStrategy>() {
                    @Override
                    public boolean filter(EventSkipStrategy value, Context<EventSkipStrategy> ctx) {
                        return value.getId().contains("b");
                    }
                });


        PatternStream<EventSkipStrategy> idEventSkipStrategyStreamNext = CEP.pattern(inputEventSkipStrategyStream, warningPatternNext);

        DataStream<Map<String, List<EventSkipStrategy>>> warningsNext = idEventSkipStrategyStreamNext.select(new PatternSelectFunction<EventSkipStrategy, Map<String, List<EventSkipStrategy>>>() {
            @Override
            public Map<String, List<EventSkipStrategy>> select(Map<String, List<EventSkipStrategy>> pattern) throws Exception {
                return pattern;
            }
        });

        warningsNext.print();
        env.execute("monitoring job");
    }
}
