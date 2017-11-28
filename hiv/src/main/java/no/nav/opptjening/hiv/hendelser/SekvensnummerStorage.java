package no.nav.opptjening.hiv.hendelser;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class SekvensnummerStorage {

    private static final Logger LOG = LoggerFactory.getLogger(SekvensnummerStorage.class);

    private final Producer<String, Long> producer;
    private final Consumer<String, Long> consumer;
    private final TopicPartition partition;

    private ConsumerRecord<String, Long> currentSekvensnummerRecord;

    public SekvensnummerStorage(Producer<String, Long> producer, Consumer<String, Long> consumer, TopicPartition partition) {
        this.producer = producer;
        this.consumer = consumer;
        this.partition = partition;

        consumer.assign(Collections.singletonList(partition));
    }

    public long getSekvensnummer() {
        if (currentSekvensnummerRecord == null) {
            currentSekvensnummerRecord = getNextSekvensnummer();

            LOG.info("Returning offset = {}, value = {}", currentSekvensnummerRecord.offset(), currentSekvensnummerRecord.value());
        }

        return currentSekvensnummerRecord.value();
    }

    private ConsumerRecord<String, Long> getNextSekvensnummer() {
        LOG.info("Polling for new sekvensnummer");
        ConsumerRecords<String, Long> consumerRecords = consumer.poll(500);

        if (consumerRecords.isEmpty()) {
            throw new IllegalStateException("Poll returned zero elements");
        }

        List<ConsumerRecord<String, Long>> records = consumerRecords.records(partition);

        int count = records.size();
        return records.get(count - 1);
    }

    public void persistSekvensnummer(long sekvensnummer) {
        producer.send(new ProducerRecord<>(partition.topic(), partition.partition(),
                "offset", sekvensnummer + 1));
        consumer.commitAsync();

        currentSekvensnummerRecord = null;
    }
}
