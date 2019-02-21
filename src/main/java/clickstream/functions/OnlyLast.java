package clickstream.functions;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import clickstream.source.ClickEvent;

/**
 * Created by flink on 15/02/17.
 */
public class OnlyLast implements Evictor<ClickEvent,TimeWindow> {
    public OnlyLast(int i) {
    }

    public void evictBefore(Iterable<TimestampedValue<ClickEvent>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

    }

    public void evictAfter(Iterable<TimestampedValue<ClickEvent>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

    }
}
