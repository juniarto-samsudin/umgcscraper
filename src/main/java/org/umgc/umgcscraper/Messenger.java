/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author samsudinj
 */
public class Messenger {
    ProducerRecord<String, String> record;
    Producer<String, String> producer;
    
    Messenger(String topicName, String key, String value){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.20.116.17:9092,172.20.116.18:9092,172.20.116.19:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
        this.record = new ProducerRecord<>(topicName, key, value);
    }
    
    //ASYNCHRONOUS
    public void async_send(){
        producer.send(record, new ProducerCallBack());
        System.out.println("async_send executed...waiting for result");
        producer.close();
    }
    
    public void send(){
        RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
            System.out.println("Message is sent to Partition no: " + metadata.partition() + " and offset " + metadata.offset());
            System.out.println("Sending Success");
        } catch (InterruptedException | ExecutionException ex) {
            Logger.getLogger(Messenger.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Sending failed!!!");
        }finally{
            producer.close();
        }
    }
    
    //ASYNCHRONOUS
    class ProducerCallBack implements Callback{

        @Override
        public void onCompletion(RecordMetadata rm, Exception excptn) {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            if (excptn != null){
                System.out.println("async_send Failed!");
            }else{
                System.out.println("async_send Success!");
            }
        }
        
    }
    
   
}
