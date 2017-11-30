package no.nav.opptjening.hiv.hendelser;

import org.apache.kafka.clients.consumer.*;
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
            try {
                currentSekvensnummerRecord = getNextSekvensnummer();
                LOG.debug("Returning offset = {}, value = {}", currentSekvensnummerRecord.offset(), currentSekvensnummerRecord.value());
            } catch (NoOffsetForPartitionException e) {
                // TODO: there must be a better way of doing this
                consumer.seekToBeginning(Collections.singletonList(partition));
                throw e;
            }
        }

        return currentSekvensnummerRecord.value();
    }

    private ConsumerRecord<String, Long> getNextSekvensnummer() {
        ConsumerRecords<String, Long> consumerRecords = consumer.poll(500);

        if (consumerRecords.isEmpty()) {
            throw new IllegalStateException("Poll returned zero elements");
        }

        List<ConsumerRecord<String, Long>> records = consumerRecords.records(partition);

        int count = records.size();
        return records.get(count - 1);
    }

    public void persistSekvensnummer(long sekvensnummer) {
        LOG.info("Producing sekvensnummer record with value={}", sekvensnummer + 1);
        producer.send(new ProducerRecord<>(partition.topic(), partition.partition(),
                "offset", sekvensnummer + 1), (recordMetadata, e) -> {
            if (e != null) {
                // seek to last committed position, effectively "rolling back"
                long offset = consumer.committed(partition).offset() + 1;
                consumer.seek(partition, offset);

                LOG.error("Error during sekvensnummer producing. Rolling back to offset {}", e, offset);
            } else {
                // commit everything up to the above message, so that it is the
                // next record to be returned by poll()
                consumer.commitAsync(Collections.singletonMap(partition, new OffsetAndMetadata(recordMetadata.offset())), (offsets, exception) -> {
                    if (exception != null) {
                        // it's ok if we fail to commit the offsets:
                        // - the above produced record will still be returned by poll()
                        // - if the consumer dies, it will continue from the last committed offset and produce a (hopefully) small amount of duplicates
                        LOG.error("Error during syncing of offsets", exception);
                    } else {
                        LOG.debug("Sekvensnummer offsets synced OK");
                    }
                });
            }
        });
        currentSekvensnummerRecord = null;
    }
}
