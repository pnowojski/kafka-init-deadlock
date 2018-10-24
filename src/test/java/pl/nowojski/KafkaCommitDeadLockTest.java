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

import java.util.Optional;
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
        String transactionalId = UUID.randomUUID().toString();
        Properties properties = new Properties();
        properties.putAll(PROPERTIES);
        properties.put("transactional.id", transactionalId);

        try {
            Producer<String, String> kafkaProducer = new KafkaProducer<>(properties);

            kafkaProducer.initTransactions();
            kafkaProducer.beginTransaction();
            for (int w = 0; w < writesCount; w++) {
                String message = Integer.toString(w);
                kafkaProducer.send(new ProducerRecord<>(topicName, message, message));
            }
            kafkaProducer.flush();

            Producer<String, String> kafkaProducer2 = new KafkaProducer<>(properties);
            kafkaProducer2.initTransactions();
            kafkaProducer2.beginTransaction();
            String message = "This shouldn't be visible";
            kafkaProducer2.send(new ProducerRecord<>(topicName, message, message));

            failRandomBroker();

            kafkaProducer2.send(new ProducerRecord<>(topicName, message, message));

            kafkaProducer.commitTransaction();
        }
        catch (Exception ex) {
            System.err.println(ex);
        }
    }

    private int failRandomBroker() {
        KafkaServer toShutDown = ENVIRONMENT.getBrokers().get(RANDOM.nextInt(ENVIRONMENT.getBrokers().size()));
        int brokerId = toShutDown.config().brokerId();
        toShutDown.shutdown();
        toShutDown.awaitShutdown();
        return brokerId;
    }
}
