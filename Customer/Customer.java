import java.util.*;
import org.apache.kafka.clients.producer.*;
public class Customer {
                                        
    public static void main(String[] args) throws Exception{
                                        
        String topicName = "AvroClicks";
        String msg;
        String timeStamp = "curr time";//new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
                                        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("max.in.flight.requests.connection","40000");

        props.put("schema.registry.url", "http://localhost:8081");


        /*Producer<String, ClickRecord> producer = new KafkaProducer <>(props);
        ClickRecord cr = new ClickRecord();
        try{
            cr.setSessionId("10001");
            cr.setChannel("HomePage");
            cr.setIp("192.168.0.1");

            producer.send(new ProducerRecord<String, ClickRecord>(topicName,cr.getSessionId().toString(),cr)).get();

            System.out.println("Complete");
        }*/

int n=5000;
int u;
        Producer<String, TxnRecord> producer = new KafkaProducer <>(props);
        TxnRecord cr = new TxnRecord();
        try{

            for(u=0; u<n;u++){
                String i=Integer.toString(u);
            cr.setSessionId(i);
            cr.setTimestamp(timeStamp);
                                                    
            producer.send(new ProducerRecord<String, TxnRecord>(topicName,cr.getSessionId().toString(),cr)).get();                
            System.out.println("Complete");
            Thread.sleep(1000/n);
        System.out.println(u);}

    }
        catch(Exception ex){
            ex.printStackTrace(System.out);
        }
        finally{
            producer.close();
        }
                                        
    }
}                      