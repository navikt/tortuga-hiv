package no.nav.opptjening.hiv.sekvensnummer;

import no.nav.opptjening.hiv.KafkaConfiguration;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaSekvensnummerWriterTest {

    private MockProducer<String, Long> producer;
    private TopicPartition partition;
    private KafkaSekvensnummerWriter writer;

    @Before
    public void setUp() {
        producer = new MockProducer<>();
        partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
        writer = new KafkaSekvensnummerWriter(producer, partition);
    }

    @Test
    public void successfulWrite() {
        writer.writeSekvensnummer(1);

        producer.completeNext();

        List<ProducerRecord<String, Long>> history = producer.history();
        List<ProducerRecord<String, Long>> expected = Collections.singletonList(new ProducerRecord<>(partition.topic(), partition.partition(), KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long) 1));

        assertEquals(expected, history);
    }

    @Test
    public void failedWrite() {
        writer.writeSekvensnummer(1);

        RuntimeException exception = new RuntimeException("Failed to write");
        producer.errorNext(exception);

        List<ProducerRecord<String, Long>> history = producer.history();
        List<ProducerRecord<String, Long>> expected = Collections.singletonList(new ProducerRecord<>(partition.topic(), partition.partition(), KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long) 1));

        assertEquals(expected, history);

        assertTrue(producer.closed());
    }
}
