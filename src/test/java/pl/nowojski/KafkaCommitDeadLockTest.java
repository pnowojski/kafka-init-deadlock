/**
 * Created by pnowojski on 24/10/2018.
 */

package pl.nowojski;

import kafka.server.KafkaServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
            KafkaProducer<String, String> kafkaProducer1 = new KafkaProducer<>(getProperties());

            kafkaProducer1.initTransactions();
            kafkaProducer1.beginTransaction();
            for (int w = 0; w < writesCount; w++) {
                String message = Integer.toString(w);
                kafkaProducer1.send(new ProducerRecord<>(topicName, message, message));
            }
            kafkaProducer1.flush();

            KafkaProducer<String, String> kafkaProducer2 = new KafkaProducer<>(getProperties());

            kafkaProducer2.initTransactions();
            kafkaProducer2.beginTransaction();
            for (int w = 0; w < writesCount; w++) {
                String message = Integer.toString(w);
                kafkaProducer2.send(new ProducerRecord<>(topicName, message, message));
            }
            kafkaProducer2.flush();

            KafkaProducer<String, String> kafkaProducer3 = new KafkaProducer<>(getProperties());
            kafkaProducer3.initTransactions();
            kafkaProducer3.beginTransaction();
            String message = "This shouldn't be visible";
            kafkaProducer3.send(new ProducerRecord<>(topicName, message, message));

            //failRandomBroker();
            failBroker(getNodeId(kafkaProducer3));

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
        toShutDown.shutdown();
        toShutDown.awaitShutdown();
        return toShutDown.config().brokerId();
    }

    private void failBroker(int brokerId) {
        KafkaServer toShutDown = null;
        for (KafkaServer server : ENVIRONMENT.getBrokers()) {
            if (server.config().brokerId() == brokerId) {
                toShutDown = server;
                break;
            }
        }

        if (toShutDown == null) {
            throw new IllegalArgumentException("Cannot find broker to shut down");
        }
        toShutDown.shutdown();
        toShutDown.awaitShutdown();
    }

    private int getNodeId(KafkaProducer<?, ?> kafkaProducer) {
        Object transactionManager = getValue(kafkaProducer, "transactionManager");
        Node node = (Node) invoke(transactionManager, "coordinator", FindCoordinatorRequest.CoordinatorType.TRANSACTION);
        return node.id();
    }

    protected static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    protected static Object getValue(Object object, String fieldName) {
        return getValue(object, object.getClass(), fieldName);
    }

    private static Object getValue(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }
}
