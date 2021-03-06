package producer;

import constant.KafkaConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author shuang.kou
 */
public class ProducerCreator {
    private static final String TOPIC = "test-topic";

    public static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BROKER_LIST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConstants.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {

        Producer<String, String> producer = ProducerCreator.createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello, Kafka!");
        try {
             //send message
             RecordMetadata metadata = producer.send(record).get();
             System.out.println("Record sent to partition " + metadata.partition()
                                + " with offset " + metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
             System.out.println("Error in sending record");
             e.printStackTrace();
        }
        producer.close();
    }







}