package clickstream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import clickstream.source.*;
import clickstream.functions.*;

/**
 * Created by Ruben Casaddo on 10/02/19.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //event-time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

       //cada cuánto se llama al método del ClickEventTimeMAanger de generar PeriodicWaterman
        env.getConfig().setAutoWatermarkInterval(100);

        //añadir el origen de datos para crear el primer DataStream
        DataStream<ClickEvent> data = env.addSource(new ClickStreamSource());

        data.assignTimestampsAndWatermarks(new ClickEventTimeManager())
                .keyBy (new ClickEventKeyGenerator())
                .timeWindow(Time.seconds(5))
                .trigger(new TriggerTop(5))
                .evictor(new LastElements(3) )
                .apply(new TopAg())
                .print();



        env.execute(" ClickStream  Flink 1.7.1 ");
    }


}
