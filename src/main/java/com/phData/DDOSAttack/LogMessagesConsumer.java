package com.phData.DDOSAttack;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LogMessagesConsumer {
    public static void main(String[] args) {
        String outputFileName = args[0];
        String bootStrapServer = args[1];
        String consumerGroup = args[2];
        String topicName = args[3];

        LogMessagesConsumer logMessagesConsumer = new LogMessagesConsumer();
        logMessagesConsumer.runConsumer(outputFileName,bootStrapServer, consumerGroup,topicName);
    }


    public void runConsumer(String outputFileName, String bootStrapServer, String consumerGroup, String topicName){
        Consumer<String, String> logMessagesConsumer = ConsumerCreator.createConsumer(bootStrapServer, consumerGroup, topicName);
        HashMap<String, Integer> map = new HashMap<>();
        File file = new File(outputFileName);
        String propertyFileName = "config.properties";

        try(BufferedWriter br = new BufferedWriter(new FileWriter(file,true))){
            Properties properties = new Properties();
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertyFileName);

            if(inputStream!=null){
                properties.load(inputStream);
            }else{
                throw new FileNotFoundException("Property File "+propertyFileName+" not found");
            }

            long t= System.currentTimeMillis();
            long end = t+Long.parseLong(properties.getProperty("TimeWindowDurationInSeconds"))*1000;

            while (System.currentTimeMillis() < end) {
                ConsumerRecords<String, String> consumerRecords = logMessagesConsumer.poll(1000);

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    String key = record.key();
                    if (null == map.get(key)) {
                        map.put(key, 1);
                    } else {
                        map.put(key, map.get(key) + 1);
                    }
                }
                logMessagesConsumer.commitAsync();
            }

            int thresholdValueOfServerHits = Integer.parseInt(properties.getProperty("ThresholdValueOfServerHits"));

            for(Map.Entry entry:map.entrySet()){
                if (map.get(entry.getKey()) > thresholdValueOfServerHits){
                    br.append(entry.getKey()+"\n");
                }
            }

        }catch (Exception e){
            System.out.println("Exception: "+e);
        }

    }
}