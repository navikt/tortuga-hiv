package no.nav.opptjening.hiv.hendelser;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class KafkaSekvensnummerWriter implements SekvensnummerWriter {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSekvensnummerWriter.class);

    static final String NEXT_SEKVENSNUMMER_KEY = "nextSekvensnummer";

    private final Producer<String, Long> producer;
    private final TopicPartition topicPartition;

    public KafkaSekvensnummerWriter(Producer<String, Long> producer, TopicPartition topicPartition) {
        this.producer = producer;
        this.topicPartition = topicPartition;
    }

    public void writeSekvensnummer(long sekvensnummer) {
        LOG.info("Writing sekvensnummer={}", sekvensnummer);

        ProducerRecord<String, Long> record = new ProducerRecord<>(topicPartition.topic(),
                topicPartition.partition(), NEXT_SEKVENSNUMMER_KEY, sekvensnummer);
        producer.send(record, new ProducerCallback(producer, record));
    }

    private static class ProducerCallback implements Callback {
        private final Producer<String, Long> producer;
        private final ProducerRecord<String, Long> record;

        private ProducerCallback(Producer<String, Long> producer, ProducerRecord<String, Long> record) {
            this.producer = producer;
            this.record = record;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                LOG.error("Error while sending sekvensnummer={}. Shutting down.", record.value(), e);
                producer.close(0, TimeUnit.MILLISECONDS);
            } else {
                LOG.info("Sekvensnummer={} sent with offset = {}", record.value(), recordMetadata.offset());
            }
        }
    }
}
