import java.util.*;
import org.apache.kafka.clients.consumer.*;                                            
import java.sql.*;                                        
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
                        //for saving and commiting manually
                        saveAndCommit(consumer, record);
                    }
                }
            }catch(Exception ex){
                ex.printStackTrace();
            }
            finally{
                consumer.close();
            }
    }
  
  
    private static void saveAndCommit(KafkaConsumer<String, Supplier> c, ConsumerRecord<String, Supplier> r){
		try{
      Class.forName("com.mysql.jdbc.Driver");
      Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/TxnRecord","root","panda");
      con.setAutoCommit(false);
                                        
      String insertSQL = "insert into Transaction values(?,?)";
      PreparedStatement psInsert = con.prepareStatement(insertSQL);
      psInsert.setInt(1,r.value().getID());
      psInsert.setString(2,r.value().getName());
                                        
      psInsert.executeUpdate();
      con.commit();
      con.close();
    }catch(Exception e){
      e.printStackTrace();
      System.out.println("Exception in saveAndCommit");
    }
	}
                                        
}                                                   
                         
