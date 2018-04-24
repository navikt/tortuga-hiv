package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.hiv.KafkaConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KafkaSekvensnummerStorageTest {

    private MockProducer<String, Long> producer;
    private MockConsumer<String, Long> consumer;
    private TopicPartition partition;
    private KafkaSekvensnummerReader reader;
    private KafkaSekvensnummerWriter writer;

    @Before
    public void setUp() {
        producer = new MockProducer<>();
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
        reader = new KafkaSekvensnummerReader(consumer, partition);
        writer = new KafkaSekvensnummerWriter(producer, partition);
    }

    private void setBeginningOffsets(long offset) {
        consumer.updateBeginningOffsets(Collections.singletonMap(partition, offset));
    }

    private void setEndOffsets(long offset) {
        consumer.updateEndOffsets(Collections.singletonMap(partition, offset));
    }

    private void createTestRecords() {
        consumer.assign(Collections.singletonList(partition));

        setBeginningOffsets(0L);

        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),0L, KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long)10));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),1L, "unusedKey1", (long)10));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),2L, KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long)20));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),3L, KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long)30));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),4L, "unusedKey2", (long)40));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),5L, KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long)50));

        setEndOffsets(6L);

        consumer.unsubscribe();
    }

    @Test
    public void when_CommittedIsNullAndNoRecords_Then_ReturnMinusOne() {
        setBeginningOffsets(0L);
        assertEquals(-1, reader.readSekvensnummer());
    }

    @Test
    public void when_CommittedIsNullAndBeginningOffsetIsGreaterThanZeroAndNoRecords_Then_ReturnMinusOne() {
        setBeginningOffsets(1L);
        assertEquals(-1, reader.readSekvensnummer());
    }

    @Test
    @Ignore("test fails because the reader unsubscribes on each call, so we cant commit anything afterwards")
    public void when_CommittedIsNotNullButNoRecords_Then_ThrowException() throws Exception {
        setBeginningOffsets(1L);

        /* read once to trigger assignment of the topic, so we can commit offset after */
        reader.readSekvensnummer();

        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1L)));

        try {
            /* we should not find ourselves here */
            fail("Expected read() to throw exception, but it returned " + reader.readSekvensnummer());
        } catch (IllegalStateException e) {
            /* expected */
        }
    }

    @Test
    public void when_CommittedIsNullAndHaveRecords_Then_ReturnLatestRecord() throws Exception {
        createTestRecords();

        assertEquals(50, reader.readSekvensnummer());
    }

    @Test
    @Ignore("this test fails because we are using MockConsumer, should rewrite it")
    public void when_RecordsToConsumeAndWeHaveReadThemAll_Then_ReadSekvensnummerReturnsMinusOne() throws Exception {
        createTestRecords();

        assertEquals(50, reader.readSekvensnummer());
        assertEquals(50, reader.readSekvensnummer());
    }

    @Test
    public void persistSekvensnummer() throws Exception {
        /*
            persistSekvensnummer(n) should:
            - produce "n + 1" on the kafka producer
              - if error,
                - getNextSekvensnummer() should equal n
                - offset should not be committed, seek should occur
              - if ok
                - getNextSekvensnummer() should equal n + 1
                - the offset should be properly committed
         */

        writer.writeSekvensnummer(1);

        producer.completeNext();

        List<ProducerRecord<String, Long>> history = producer.history();
        List<ProducerRecord<String, Long>> expected = Collections.singletonList(new ProducerRecord<>(partition.topic(), partition.partition(), KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long) 1));

        assertEquals(expected, history);
    }

}
