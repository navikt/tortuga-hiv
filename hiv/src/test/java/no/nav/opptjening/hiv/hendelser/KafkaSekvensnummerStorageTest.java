package no.nav.opptjening.hiv.hendelser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KafkaSekvensnummerStorageTest {

    private MockProducer<String, Long> producer;
    private MockConsumer<String, Long> consumer;
    private TopicPartition partition;
    private KafkaSekvensnummerStorage storage;

    @Before
    public void setUp() {
        producer = new MockProducer<>();
        consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        partition = new TopicPartition("tortuga.inntektshendelser.offsets", 0);

        storage = new KafkaSekvensnummerStorage(producer, consumer, partition);
    }

    private void setBeginningOffsets() {
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(partition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
    }

    private void createTestRecords() {
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),0L, "offset", (long)10));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),1L, "offset", (long)20));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),2L, "offset", (long)30));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),3L, "offset", (long)40));
        consumer.addRecord(new ConsumerRecord<>(partition.topic(), partition.partition(),4L, "offset", (long)50));
    }

    @Test(expected = NoOffsetForPartitionException.class)
    public void when_HaveNoOffsets_Then_ThrowException() {
        storage.getSekvensnummer();
    }

    @Test
    public void when_HaveNoOffset_Then_SeekToBeginning() {
        setBeginningOffsets();

        try {
            storage.getSekvensnummer();
            /* we should not find ourselves here */
            fail("Expected getSekvensnummer() to throw exception");
        } catch (NoOffsetForPartitionException e) {
            /* expected */
        }

        assertEquals(0, consumer.position(partition));
    }

    @Test
    public void when_NothingToConsume_Then_ThrowException() throws Exception {
        setBeginningOffsets();

        try {
            storage.getSekvensnummer();
            /* we should not find ourselves here */
            fail("Expected getSekvensnummer() to throw exception");
        } catch (NoOffsetForPartitionException e) {
            /* expected */
        }

        try {
            storage.getSekvensnummer();
            fail("Expected getSekvensnummer to throw exception");
        } catch (IllegalStateException e) {
            assertEquals("Poll returned zero elements", e.getMessage());
        }
    }

    @Test
    public void when_HaveNoOffset_Then_SeekToBeginning_And_ReturnLatestRecord() throws Exception {
        setBeginningOffsets();

        try {
            storage.getSekvensnummer();
            /* we should not find ourselves here */
            fail("Expected getSekvensnummer() to throw exception");
        } catch (NoOffsetForPartitionException e) {
            /* expected */
        }

        createTestRecords();

        assertEquals(50, storage.getSekvensnummer());
    }

    @Test
    public void when_RecordsToConsume_that_ConsecutiveCallsToGetSekvensnummer_Returns_The_Same() throws Exception {
        setBeginningOffsets();

        try {
            storage.getSekvensnummer();
            /* we should not find ourselves here */
            fail("Expected getSekvensnummer() to throw exception");
        } catch (NoOffsetForPartitionException e) {
            /* expected */
        }

        createTestRecords();

        assertEquals(50, storage.getSekvensnummer());
        assertEquals(50, storage.getSekvensnummer());
    }

    @Test
    public void testInitial() throws Exception {
        setBeginningOffsets();

        try {
            storage.getSekvensnummer();
            /* we should not find ourselves here */
            fail("Expected getSekvensnummer() to throw exception");
        } catch (NoOffsetForPartitionException e) {
            storage.persistSekvensnummer(0);
        }

        producer.flush();

        long offset = 0;
        for (ProducerRecord<String, Long> record : producer.history()) {
            consumer.addRecord(new ConsumerRecord<>(record.topic(), record.partition(), offset++, record.key(), record.value()));
        }

        assertEquals(1, storage.getSekvensnummer());
        assertEquals(0, consumer.committed(partition).offset());
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

        storage.persistSekvensnummer(1);

        producer.completeNext();

        List<ProducerRecord<String, Long>> history = producer.history();
        List<ProducerRecord<String, Long>> expected = Collections.singletonList(new ProducerRecord<>(partition.topic(), partition.partition(),"offset", (long) 2));

        assertEquals(expected, history);
    }

}
