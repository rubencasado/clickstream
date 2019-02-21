package clickstream.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import clickstream.source.ClickEvent;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by flink on 14/02/17.
 */
public class TopPageAggregation implements WindowFunction<ClickEvent, Tuple2<String, Integer>, Tuple, TimeWindow> {
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<ClickEvent> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {


        Iterator itr = iterable.iterator();
        ClickEvent event;
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        int cont;
        while (itr.hasNext()){
             event = (ClickEvent) itr.next();
            if (map.containsKey(event.page)){
                cont = map.get(event.page).intValue();
                cont++;
                map.put(event.page, Integer.valueOf(cont));
                System.out.println(event);
            }
            else {
                map.put(event.page, 1);
                System.out.println(event);

            }

        }
        String key, maxKey=null;
        int maxValue=0;
        Iterator<String> domains= map.keySet().iterator();
        while (itr.hasNext()){
            key = (String) itr.next();
            if (map.get(key).intValue() > maxValue){
                maxValue = map.get(key).intValue();
                maxKey = key;
            }
        }
        collector.collect(new Tuple2<String, Integer>(maxKey,maxValue));

    }
}
