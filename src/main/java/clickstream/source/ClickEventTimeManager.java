package clickstream.source;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Created by flink on 14/02/17.
 */
public class ClickEventTimeManager implements AssignerWithPeriodicWatermarks<ClickEvent> {

    private final long maxTimeLag = 5000; // 2 seconds


    public Watermark getCurrentWatermark() {
        // return the watermark as current time - the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }


    public long extractTimestamp(ClickEvent clickEvent, long l) {

        return Long.parseLong(clickEvent.timestamp);
    }
}
