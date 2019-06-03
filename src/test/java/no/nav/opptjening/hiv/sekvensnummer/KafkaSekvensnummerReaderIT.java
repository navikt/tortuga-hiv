package no.nav.opptjening.hiv.sekvensnummer;

import no.nav.common.KafkaEnvironment;
import no.nav.opptjening.hiv.KafkaConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaSekvensnummerReaderIT {

    private static final int NUMBER_OF_BROKERS = 3;
    private static final List<String> TOPICS = Collections.singletonList(KafkaConfiguration.SEKVENSNUMMER_TOPIC);

    private static KafkaEnvironment kafkaEnvironment;
    private static KafkaConfiguration kafkaConfiguration;

    private static Producer<String, Long> sekvensnummerProducer;
    private static Consumer<String, Long> sekvensnummerConsumer;

    private static final TopicPartition partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
    private static final List<TopicPartition> partitionList = Collections.singletonList(partition);

    @BeforeAll
    public static void setUp() {
        kafkaEnvironment = new KafkaEnvironment(NUMBER_OF_BROKERS, TOPICS, false, false, Collections.emptyList(),false);
        kafkaEnvironment.start();

        Map<String, String> env = new HashMap<>();
        env.put(KafkaConfiguration.Properties.BOOTSTRAP_SERVERS, kafkaEnvironment.getBrokersURL());
        env.put(KafkaConfiguration.Properties.SECURITY_PROTOCOL, "PLAINTEXT");

        kafkaConfiguration = new KafkaConfiguration(env);

        sekvensnummerProducer = kafkaConfiguration.offsetProducer();
        sekvensnummerConsumer = kafkaConfiguration.offsetConsumer();
    }

    @AfterAll
    public static void tearDown() {
        kafkaEnvironment.tearDown();
    }

    private KafkaSekvensnummerReader getSekvensnummerReader() {
        return new KafkaSekvensnummerReader(sekvensnummerConsumer, partition);
    }

    private void createTestRecords() {
        KafkaSekvensnummerWriter writer = new KafkaSekvensnummerWriter(sekvensnummerProducer, partition);

        writer.writeSekvensnummer(1);
        writer.writeSekvensnummer(11);
        writer.writeSekvensnummer(21);

        sekvensnummerProducer.flush();
    }

    private void createBadRecords() {
        List<ProducerRecord<String, Long>> records = Arrays.asList(
                new ProducerRecord<>(partition.topic(), partition.partition(), "badKey1", 1L),
                new ProducerRecord<>(partition.topic(), partition.partition(), "badKey2", 2L),
                new ProducerRecord<>(partition.topic(), partition.partition(), "badKey3", 3L)
        );

        for (ProducerRecord<String, Long> record : records) {
            sekvensnummerProducer.send(record);
        }
        sekvensnummerProducer.flush();
    }

    private void createTestRecordsWithBadRecords() {
        createBadRecords();
        createTestRecords();
    }

    @Test
    @Order(1)
    public void when_CommittedIsNullAndNoRecords_Then_ReturnMinusOne() {
        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertEquals(Optional.empty(), reader.readSekvensnummer());
    }

    @Test
    @Order(2)
    public void when_CommittedIsNullAndNoRecords_Then_ReReading_Should_ReturnMinusOne() {
        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertEquals(Optional.empty(), reader.readSekvensnummer());
        assertEquals(Optional.empty(), reader.readSekvensnummer());
    }

    @Test
    @Order(3)
    public void when_CommittedIsNotNullAndNoRecords_Then_Throw() {
        KafkaSekvensnummerReader reader = getSekvensnummerReader();
        sekvensnummerConsumer.assign(partitionList);
        sekvensnummerConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1)));

        assertThrows(NoNextSekvensnummerRecordsToConsume.class, reader::readSekvensnummer);
    }

    @Test
    @Order(4)
    public void when_CommittedIsNotNullAndNoRecordsWithCorrectKey_Then_Throw() {
        createBadRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        sekvensnummerConsumer.assign(partitionList);
        sekvensnummerConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1)));

        assertThrows(CouldNotFindNextSekvensnummerRecord.class, reader::readSekvensnummer);
    }

    @Test
    public void when_CommittedIsNullAndRecords_Then_ReturnLastOne() {
        createTestRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNullAndRecords_Then_ReReading_Should_ReturnLastOne() {
        createTestRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNullAndRecordsWithBadRecords_Then_ReturnLastOne() {
        createTestRecordsWithBadRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNullAndRecordsWithBadRecords_Then_ReReading_ReturnLastOne() {
        createTestRecordsWithBadRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNotNullAndRecords_Then_ReturnLastOne() {
        createTestRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        sekvensnummerConsumer.assign(partitionList);
        sekvensnummerConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1)));

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNotNullAndRecords_Then_ReReading_ReturnLastOne() {
        createTestRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        sekvensnummerConsumer.assign(partitionList);
        sekvensnummerConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1)));

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNotNullAndRecordsWithBadRecords_Then_ReturnLastOne() {
        createTestRecordsWithBadRecords();

        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        sekvensnummerConsumer.assign(partitionList);
        sekvensnummerConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1)));

        assertEquals(Optional.of(21L), reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNullAndNoRecordsWithCorrectKey_Then_Throw() {
        createBadRecords();
        KafkaSekvensnummerReader reader = getSekvensnummerReader();

        assertThrows(CouldNotFindNextSekvensnummerRecord.class, reader::readSekvensnummer);
    }

}
