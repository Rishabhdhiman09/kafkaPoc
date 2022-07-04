package kafkaPoc;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageSender {
	Properties props = new Properties();
	
	private void init() throws InterruptedException{
		
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("kafka.topic.name", "kafkaTopic");
		
		// when ever producer send data to broker it serialize the data first
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(this.props, 
				new StringSerializer(), new ByteArraySerializer());
		
		
		for (int i = 0; i < 10; i++) {
			
			byte[] payload = (i + "message from java code" + new Date()).getBytes();
			System.out.println(i + "message from java code" + new Date());
			
			ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>
								(props.getProperty("kafka.topic.name"), payload);
								
			producer.send(record);
			Thread.sleep(1000);
		}
		producer.close();
	}
	
	public static void main(String[] args) {
		MessageSender messageSender = new MessageSender();
		
		 try {
			messageSender.init();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
