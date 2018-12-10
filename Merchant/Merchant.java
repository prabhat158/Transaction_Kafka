import java.util.*;
import org.apache.kafka.clients.consumer.*;                                            
                                        
public class Merchant{
                                        
    public static void main(String[] args) throws Exception{
                                        
        String topicName = "AvroClicks";                                            
        String groupName = "RG";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true");
                                        
        KafkaConsumer<String, TxnRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        try{
            while (true){
                ConsumerRecords<String, TxnRecord> records = consumer.poll(100);
                //if(records != null) System.out.println("Hello");
                for (ConsumerRecord<String, TxnRecord>record : records){
                        //System.out.println("Hello");
                        System.out.println("Session id="+ record.value().getSessionId()
                                        + " Timestamp=" + record.value().getTimestamp() );
                    }
                }
            }catch(Exception ex){
                ex.printStackTrace();
            }
            finally{
                consumer.close();
            }
    }
                                        
}                                                   
                         