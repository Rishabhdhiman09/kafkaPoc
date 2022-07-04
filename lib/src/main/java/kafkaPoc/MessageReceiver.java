package kafkaPoc;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class MessageReceiver {
	
	private static final String TOPIC = "kafkaTopic";
	private static final String GROUP = "kafkaTopic_group";
	
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", GROUP);
		
		// also called offset commit, it means how much data is already taken from broker to 
		// consumer. It updates this information to kafka server
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
			consumer.subscribe(Arrays.asList(TOPIC));
			
				
			ConsumerRecords<String, String> records = consumer.poll(1000L);
			System.out.println(records.count());
			
			for (ConsumerRecord<String, String> record : records) {
				
				System.out.println("receives a message: " + record.value());
			}
			
			
		}
		System.out.println("end");

		
	}
}
