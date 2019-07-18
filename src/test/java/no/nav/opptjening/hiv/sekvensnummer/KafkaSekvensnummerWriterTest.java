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

class KafkaSekvensnummerWriterTest {

    private static MockProducer<String, Long> producer;
    private static TopicPartition partition;
    private static KafkaSekvensnummerWriter writer;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>();
        partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
        writer = new KafkaSekvensnummerWriter(producer, partition);
    }

    @Test
    void successfulWrite() {
        writer.writeSekvensnummer(1);

        producer.completeNext();

        List<ProducerRecord<String, Long>> history = producer.history();
        List<ProducerRecord<String, Long>> expected = Collections.singletonList(new ProducerRecord<>(partition.topic(), partition.partition(), KafkaSekvensnummerReader.NEXT_SEKVENSNUMMER_KEY, (long) 1));

        assertEquals(expected, history);
    }
}
