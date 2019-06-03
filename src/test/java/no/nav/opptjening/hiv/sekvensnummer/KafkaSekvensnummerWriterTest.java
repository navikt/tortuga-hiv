package no.nav.opptjening.hiv.sekvensnummer;

import no.nav.opptjening.hiv.KafkaConfiguration;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaSekvensnummerWriterTest {

    private static MockProducer<String, Long> producer;
    private static TopicPartition partition;
    private static KafkaSekvensnummerWriter writer;

    @BeforeEach
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

        long timeStart = System.currentTimeMillis();
        while (!producer.closed()) {
            long now = System.currentTimeMillis();

            if ((now - timeStart) > 250) {
                throw new RuntimeException("Waited for 250 milliseconds for producer to get closed.");
            }
        }

        assertTrue(producer.closed());
    }
}
