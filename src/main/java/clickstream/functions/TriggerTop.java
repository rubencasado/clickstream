package clickstream.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import clickstream.source.ClickEvent;

/**
 * Created by flink on 15/02/17.
 */
public class TriggerTop extends Trigger<ClickEvent,TimeWindow> {

    //crear descriptor ("nombre") de State en Flink

    private ValueStateDescriptor<Integer> descriptor =
            new ValueStateDescriptor<Integer>("count", Integer.TYPE);

    private int maxElements;

    public TriggerTop(int n) {
        maxElements=n;

    }

    public TriggerResult onElement(ClickEvent clickEvent, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        ValueState<Integer> count =
                triggerContext.getPartitionedState(descriptor);

        int elements= 0;
        if (count.value()!= null)
            elements=count.value().intValue();
        count.update(elements+1);
        if (elements>=maxElements) {
            count.update(0);
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;

    }

    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
