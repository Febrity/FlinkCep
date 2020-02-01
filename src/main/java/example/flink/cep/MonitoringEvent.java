package example.flink.cep;

        import org.apache.flink.streaming.api.functions.source.SourceFunction;

        import java.util.Random;

public class MonitoringEvent implements SourceFunction<Event> {

    @Override
    public void run(SourceContext sourceContext) {

        Random random1 = new Random();

        for (int i = 0; i < 10; i++) {
            Event Event = new Event(random1.nextInt(3));
            System.out.println("Source  " + Event);
            sourceContext.collect(Event);
        }
    }

    @Override
    public void cancel() {
    }
}
