package org.example;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.scheduler.Schedulers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaSinkTest {

    private static final Logger log = LogManager.getLogger(KafkaSinkTest.class);

    private static  ArrayList<String> genData(int recordNum){
//       String randV = RandomStringUtils.randomAlphabetic(80);
        //String v = "01234567891011121314151617181920212223242526272829303132333435363738394041424344454647484950515253545556575859606162636465666768697071727374757677787980";
        ArrayList<String> dataList = new ArrayList<String>();
        log.info("prepare data");
        for (int i=0; i< recordNum;i++) {
            dataList.add(RandomStringUtils.randomAlphabetic(80));
        }
        return dataList;
    }
    private void publishPlatformLoopQps(final String broker, final String topic,final int recordNum,final int loop, boolean autoPartition,String kafkaProp, String acl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,"0");

        if ("true".equalsIgnoreCase(acl)){
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "SCRAM-SHA-512");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                    "username=\"admin\" " +
                    "password=\"admin\";");
        }


        if (!"".equals(kafkaProp)&& kafkaProp!=null){
            for (String kv : kafkaProp.split(",")){
                String k = kv.split("=")[0];
                String v = kv.split("=")[1];
                props.put(k,v);
            }
        }

        Schedulers.newSingle("pub" + topic).schedule(() -> {
            try {
                Producer<String, String> producer = new KafkaProducer<>(props);
                String key = "";

                for (int i=0; i< loop;i++){
                    ArrayList<String> dataList = genData(recordNum);
                    log.info("starting send loop data...");
                    int qps = 0;
                    long startNanoSec = System.nanoTime();
                    for (String value : dataList){
                        ProducerRecord<String, String> producerRecord = null;
                        if (autoPartition) {
                            producerRecord = new ProducerRecord<>(topic, null, value);
                        } else {
                            producerRecord = new ProducerRecord<>(topic, 0, key, value);
                        }
                        int finalQps = qps;
                        producer.send(producerRecord, (recordMetadata, e) -> {
                            if (e != null) {
                                log.error("send catch ex:{}", e.getMessage());
                            } else {
                                long  recNum = dataList.size();

                                if (finalQps+1 == dataList.size()){
                                    final long c = System.nanoTime() - startNanoSec;
                                    long ms= (c/1000000L);
                                    double sec = ms/1000.0;
                                    log.info("loop callback finished, recordNum:{}  time {} ms ,QPS {} records/s", recNum, ms,recNum/sec);
                                }
                            }
                        });
                        ++qps;
                    }
                    log.info("send loop finished {}",i+1);
                }

            } catch (Exception e) {
                log.error("catch exception", e);
            }

        });

    }
    public static void main(String[] args) {
        String broker = args[0];
        String topic = args[1];
        String recordNum = args[2];
        String loop = args[3];
        String kafkaProp = "";
        String acl = "false";
        if (args.length==5){
             kafkaProp = args[4];
        }
        if (args.length==6){
            acl = args[5];
        }

        new KafkaSinkTest().publishPlatformLoopQps(broker,topic,Integer.parseInt(recordNum),Integer.parseInt(loop),false,kafkaProp,acl);
    }
}