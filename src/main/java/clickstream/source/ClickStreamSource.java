package clickstream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by flink on 14/02/17.
 */


public class ClickStreamSource implements SourceFunction<ClickEvent> {


    public void run(SourceContext<ClickEvent> sourceContext) throws Exception {

        while (this.running){
            sourceContext.collect(this.randomEvent());
            Thread.sleep(Math.round(Math.random()*300));
        }

    }

    public void cancel() {
        this.running=false;

    }

    protected boolean running;
    protected int outOrder;

    public ClickStreamSource(){
        this.running=true;
        this.outOrder = 3;
    }

    public ClickStreamSource(int outOrder){
        this.running=true;
        if (outOrder>=0 && outOrder <=10)
            this.outOrder= outOrder;
        else
            this.outOrder = 3;
    }

    protected ClickEvent randomEvent(){
        String ip = this.getRandomIP();
        String timestamp = this.getTimeStamp(); //event time + random out-order-events
        String domain = this.getRandomDomain();
        String page = this.getRandomPage();
        String resource = this.getRandomResource();

        return new ClickEvent(ip, timestamp, domain, page, resource);
    }

    protected String getRandomIP(){

        return Integer.toString(randomInteger(0,256)) + "." +
                Integer.toString(randomInteger(0,256)) + "."+
                Integer.toString(randomInteger(0,156)) + "."+
                Integer.toString(randomInteger(0,56));
    }

    protected String getTimeStamp(){
        long eventTime = System.currentTimeMillis();
        long delay =0;
        // 30% of events are out-of-ordered
        if (randomInteger(0,10)>(10-this.outOrder))
            delay= randomInteger(1, 1000);
        return Long.toString(eventTime-delay);
    }

    protected String getRandomDomain(){

        int random = this.randomInteger(0, 7);
        switch (random){
            case 0: return "accenture.com";
            case 1: return "kschool.com";
            case 2: return "meetup.com";
            case 3: return "marca.com";
            case 4: return "elpais.es";
            case 5: return "larazon.es";
            case 7: return "google.com";
        }
        return "null";
    }

    protected String getRandomPage(){

        int random = this.randomInteger(0, 4);
        switch (random){
            case 0: return "/index.html";
            case 1: return "/index.php";
            case 2: return "/news";
            case 3: return "/notifications";
            case 4: return "/main";

        }
        return "null";
    }

    protected String getRandomResource(){

        int random = this.randomInteger(0, 4);
        switch (random){
            case 0: return "form";
            case 1: return "mouse";
            case 2: return "image";
            case 3: return "banner";
            case 4: return "other";

        }
        return "null";
    }



    protected int randomInteger(int min, int max){

        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }


}
