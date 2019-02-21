package clickstream.functions;

import clickstream.source.ClickEvent;

/**
 * Created by flink on 14/02/17.
 */
public class ClickEventKeyGenerator implements org.apache.flink.api.java.functions.KeySelector<ClickEvent, Object> {
    public Object getKey(ClickEvent clickEvent) throws Exception {
        return clickEvent.domain;
    }
}
