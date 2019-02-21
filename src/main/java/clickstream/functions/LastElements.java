package clickstream.functions;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import clickstream.source.ClickEvent;

import java.util.Iterator;

/**
 * Created by flink on 16/02/19.
 */
public class LastElements implements Evictor<ClickEvent, TimeWindow> {

    private int count;

    public LastElements (int x) {
        count=x;
    }
    public void evictBefore(Iterable<TimestampedValue<ClickEvent>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

        int evictedCount=0;
        int totalElements=i;

        for (Iterator<TimestampedValue<ClickEvent>> iterator = iterable.iterator(); iterator.hasNext();){
            iterator.next();
            evictedCount++;
            if (totalElements - evictedCount < this.count) {
                break;
            } else {
                iterator.remove();
            }
        }

    }

    public void evictAfter(Iterable<TimestampedValue<ClickEvent>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {
    }
}
