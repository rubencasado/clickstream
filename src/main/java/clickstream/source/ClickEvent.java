package clickstream.source;

/**
 * Created by flink on 14/02/17.
 */
public class ClickEvent {

    public String ip_user;
    public String timestamp;
    public String domain;
    public String page;
    public String resource;

    public ClickEvent(String ip_user, String timestamp, String domain, String page, String resource){
        this.ip_user=ip_user;
        this.timestamp=timestamp;
        this.domain=domain;
        this.page=page;
        this.resource=resource;
    }

    public String toString(){
        return "{" +
                 "\"ip_user\": \""+ ip_user+"\", "+
                "\"timestamp\": \""+ timestamp+"\", "+
                "\"domain\": \""+ domain+"\", "+
                "\"page\": \""+ page + "\", "+
                "\"resource\": \""+ resource + "\""+
                "}";
    }
}
