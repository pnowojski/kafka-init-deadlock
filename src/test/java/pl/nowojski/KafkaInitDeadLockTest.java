/**
 * Created by pnowojski on 26/10/2017.
 */

package pl.nowojski;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class KafkaInitDeadLockTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaInitDeadLockTest.class);

    private final static KafkaTestEnvironment ENVIRONMENT = new KafkaTestEnvironment();
    private final static Properties PROPERTIES = new Properties();

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
        final int testThreadsCount = 20;

        String topicName = "flink-kafka-dead-lock";

        List<KafkaTestThread> threads = new ArrayList<>();
        for (int i = 0; i < testThreadsCount; i++) {
            threads.add(new KafkaTestThread(i, topicName, PROPERTIES, i % 2 == 0));
        }
        for (int i = 0; i < testThreadsCount; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < testThreadsCount; i++) {
            threads.get(i).join();
            threads.get(i).checkException();
        }
        ENVIRONMENT.deleteTestTopic(topicName);
    }

    private static class KafkaTestThread extends Thread {
        public static int initCounts = 30;
        public static int writeCounts = 100;
        public static int iterations = 100;

        private final Properties[] properties = new Properties[initCounts];
        private final String topicName;
        private final boolean transactionalWrites;
        private final Properties nonTransactionalProperties = new Properties();

        private Optional<Exception> exception = Optional.empty();

        public KafkaTestThread(int id, String topicName, Properties defaults, boolean transactionalWrites) {
            setName(String.format("KafkaTestThread[%s, %s]", id, transactionalWrites));
            this.topicName = topicName;
            this.transactionalWrites = transactionalWrites;

            for (int i = 0; i < initCounts; i++) {
                String transactionalId = UUID.randomUUID().toString();
                Properties defaultsCopy = new Properties();
                defaultsCopy.putAll(defaults);
                defaultsCopy.put("transactional.id", transactionalId);
                this.properties[i] = defaultsCopy;
            }

            nonTransactionalProperties.putAll(defaults);
            nonTransactionalProperties.remove("transactional.id");
        }

        @Override
        public void run() {
            try {
                testLoop();
            }
            catch (Exception ex) {
                exception = Optional.of(ex);
                LOG.error("Something failed", ex);
            }
        }

        public void checkException() throws Exception {
            if (exception.isPresent()) {
                throw exception.get();
            }
        }

        private void testLoop() {
            for (int j = 0; j < iterations; j++) {
                LOG.info(getName() + " Iteration " + j);
                for (int i = 0; i < initCounts; i++) {
                    try (Producer<String, String> kafkaProducer = new KafkaProducer<>(properties[i])) {
                        kafkaProducer.initTransactions();
                    }
                }
                if (transactionalWrites) {
                    try (Producer<String, String> kafkaProducer = new KafkaProducer<>(properties[0])) {
                        kafkaProducer.initTransactions();
                        kafkaProducer.beginTransaction();
                        for (int w = 0; w < writeCounts; w++) {
                            String message = Integer.toString(w);
                            kafkaProducer.send(new ProducerRecord<>(topicName, message, message));
                        }
                        kafkaProducer.flush();
                        kafkaProducer.commitTransaction();
                    }
                }
                else {
                    try (Producer<String, String> kafkaProducer = new KafkaProducer<>(nonTransactionalProperties)) {
                        for (int w = 0; w < writeCounts; w++) {
                            String message = Integer.toString(w);
                            kafkaProducer.send(new ProducerRecord<>(topicName, message, message));
                        }
                        kafkaProducer.flush();
                    }
                }
            }
        }
    }
}
