package clickstream.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import clickstream.source.ClickEvent;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by flink on 14/02/17.
 */
public class TopAg implements org.apache.flink.streaming.api.functions.windowing.WindowFunction<ClickEvent, Tuple3<String, String, Integer>, Object, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

    public void apply(Object o, TimeWindow timeWindow, Iterable<ClickEvent> iterable, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        Iterator itr = iterable.iterator();
        ClickEvent event;
        int cont;
        String domain=null;
        while (itr.hasNext()){
            event = (ClickEvent) itr.next();
            domain= event.domain;
            if (map.containsKey(event.page)){
                cont = map.get(event.page).intValue();
                cont++;
                map.put(event.page, Integer.valueOf(cont));

            }
            else {
                map.put(event.page, 1);
            }

        }
        String key, maxKey=null;
        int maxValue=0;
        Iterator<String> domains= map.keySet().iterator();
        while (domains.hasNext()){
            key = (String) domains.next();
            if (map.get(key).intValue() >= maxValue){
                maxValue = map.get(key).intValue();
                maxKey = key;
            }
        }
        collector.collect(new Tuple3<String, String, Integer>(domain, maxKey,maxValue));
    }
}
