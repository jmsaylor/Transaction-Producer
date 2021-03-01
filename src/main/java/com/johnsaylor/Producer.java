package com.johnsaylor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Int;

import java.util.Properties;

public class Producer {
    Properties props = new Properties();
    KafkaProducer<String, String> producer;

    public Producer(){
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String tx) {
        String topic = "john";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, getAmount(tx), tx);
        try {

            producer.send(record);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public String getAmount(String tx) {
        String[] fields = tx.split(",");
        String amount = fields[2].split("\\.")[0];
        System.out.println("Kafka Producer: " + amount + " " + tx);
        return amount;
    }

    public void test() {
        String topic = "john";

        for (int i = 0; i < 100; i++) {
            System.out.println(Integer.toString(i));
            ProducerRecord record = new ProducerRecord(topic, Integer.toString(i), ("234523JOHN,:LK".concat(Integer.toString(i))));
            producer.send(record);
        }

    }

}
