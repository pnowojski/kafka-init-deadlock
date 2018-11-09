/**
 * Created by pnowojski on 24/10/2018.
 */

package pl.nowojski;

import kafka.server.KafkaServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaCommitDeadLockTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitDeadLockTest.class);

    private final static KafkaTestEnvironment ENVIRONMENT = new KafkaTestEnvironment();
    private final static Properties PROPERTIES = new Properties();
    private final static Random RANDOM = new Random();

    @BeforeClass
    public static void before() {
        ENVIRONMENT.prepare();

        PROPERTIES.putAll(ENVIRONMENT.getStandardProperties());
        PROPERTIES.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @AfterClass
    public static void after() {
        ENVIRONMENT.shutdown();
    }

    @Test
    public void testDeadLock() throws Exception {
        int executions = 1;
        int writesCount = 10;

        String topicName = "kafka-dead-lock";
        ENVIRONMENT.createTestTopic(topicName, 1, 1, new Properties());

        for (int i = 0; i < executions; i++) {
            runSingleExecution(topicName, writesCount);
        }
        ENVIRONMENT.deleteTestTopic(topicName);
    }

    private void runSingleExecution(String topicName, int writesCount) throws Exception {
        System.err.println("runSingleExecution");

        try {
            Producer<String, String> kafkaProducer1 = new KafkaProducer<>(getProperties());

            kafkaProducer1.initTransactions();
            kafkaProducer1.beginTransaction();
            for (int w = 0; w < writesCount; w++) {
                String message = Integer.toString(w);
                kafkaProducer1.send(new ProducerRecord<>(topicName, message, message));
            }
            kafkaProducer1.flush();

            Producer<String, String> kafkaProducer2 = new KafkaProducer<>(getProperties());

            kafkaProducer2.initTransactions();
            kafkaProducer2.beginTransaction();
            for (int w = 0; w < writesCount; w++) {
                String message = Integer.toString(w);
                kafkaProducer2.send(new ProducerRecord<>(topicName, message, message));
            }
            kafkaProducer2.flush();

            Producer<String, String> kafkaProducer3 = new KafkaProducer<>(getProperties());
            kafkaProducer3.initTransactions();
            kafkaProducer3.beginTransaction();
            String message = "This shouldn't be visible";
            kafkaProducer3.send(new ProducerRecord<>(topicName, message, message));

            failRandomBroker();

            kafkaProducer3.send(new ProducerRecord<>(topicName, message, message));

            kafkaProducer1.commitTransaction();
            kafkaProducer1.close();
            kafkaProducer2.commitTransaction();
            kafkaProducer2.close();

            kafkaProducer3.close();
        }
        catch (Exception ex) {
            System.err.println(ex);
        }
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.putAll(PROPERTIES);
        properties.put("transactional.id", UUID.randomUUID().toString());
        return properties;
    }

    private int failRandomBroker() {
        KafkaServer toShutDown = ENVIRONMENT.getBrokers().get(RANDOM.nextInt(ENVIRONMENT.getBrokers().size()));
        int brokerId = toShutDown.config().brokerId();
        toShutDown.shutdown();
        toShutDown.awaitShutdown();
        return brokerId;
    }
}
