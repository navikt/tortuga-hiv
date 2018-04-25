package no.nav.opptjening.hiv.hendelser;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaSekvensnummerReader implements SekvensnummerReader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSekvensnummerReader.class);

    private static final int POLL_TIMEOUT_MS = 1000;

    static final String NEXT_SEKVENSNUMMER_KEY = "nextSekvensnummer";

    private static final long INDETERMINATE = -1;

    private final Consumer<String, Long> consumer;
    private final TopicPartition topicPartition;

    private static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer_persisted")
            .help("Neste sekvensnummer vi forventer å få hendelser på, som er lagret på Kafka").register();
    private static final Gauge nextSekvensnummerOffsetGauge = Gauge.build()
            .name("next_sekvensnummer_persisted_offset")
            .help("Offset til neste sekvensnummer vi forventer å få hendelser på, som er lagret på Kafka").register();

    public KafkaSekvensnummerReader(Consumer<String, Long> consumer, TopicPartition topicPartition) {
        this.consumer = consumer;
        this.topicPartition = topicPartition;
    }

    public long readSekvensnummer() {
        consumer.assign(Collections.singletonList(topicPartition));

        OffsetAndMetadata committedOffset = consumer.committed(topicPartition);
        LOG.info("Committed offset = {}", committedOffset);

        if (committedOffset == null) {
            // AUTO_OFFSET_RESET_CONFIG kicks in.
            // TODO: should we test that it's set to "earliest"?
            LOG.info("Committed offset is null");
        } else {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(topicPartition));
            if (committedOffset.offset() == endOffsets.get(topicPartition)) {
                LOG.info("The committed offset {} is also the last offset on the log. " +
                        "Seeking to {}", committedOffset.offset(), committedOffset.offset() - 1);
                consumer.seek(topicPartition, committedOffset.offset() - 1);
            } else {
                LOG.info("Committed offset = {}, end offset = {}", committedOffset.offset(),
                        endOffsets.get(topicPartition));
            }
        }

        try {
            ConsumerRecord<String, Long> nextSekvensnummer = drain(topicPartition, NEXT_SEKVENSNUMMER_KEY);

            if (nextSekvensnummer != null) {
                LOG.debug("Found nextSekvensnummer={} (offset = {})", nextSekvensnummer.value(), nextSekvensnummer.offset());
            } else {
                LOG.error("Found no records with key={}", NEXT_SEKVENSNUMMER_KEY);
            }

            long currentPosition = consumer.position(topicPartition);

            LOG.info("Position after poll = {}", currentPosition);

            if (nextSekvensnummer == null) {
                throw new CouldNotFindNextSekvensnummerRecord("We consumed records on the topic, " +
                        "but found none with the correct key. Cannot continue.");
            }

            /* sync once we have a nextSekvensnummer record */
            OffsetAndMetadata offsetToCommit = new OffsetAndMetadata(nextSekvensnummer.offset() + 1);
            consumer.commitSync(Collections.singletonMap(topicPartition, offsetToCommit));
            LOG.info("Offset={} committed (committed offset = {})", offsetToCommit.offset(),
                    consumer.committed(topicPartition).offset());

            nextSekvensnummerOffsetGauge.set(nextSekvensnummer.offset());
            nextSekvensnummerGauge.set(nextSekvensnummer.value());

            return nextSekvensnummer.value();
        } catch (NoNextSekvensnummerRecordsToConsume e) {
            if (committedOffset == null) {
                /* if committedOffset is null, and we have configured the consumer
                    with AUTO_OFFSET_RESET_CONFIG=earliest, we can now assume that the log is actually empty */
                // TODO: should we assume AUTO_OFFSET_RESET_CONFIG=earliest or handle the seeking ourselves?
                LOG.info("No records were consumed, and committed offset was null, so we assume the log is empty");
                return INDETERMINATE;
            }

            throw e;
        } finally {
            consumer.unsubscribe();
        }
    }

    /* drain the assigned topics, reading as much as possible */
    private List<ConsumerRecords<String, Long>> drain() {
        List<ConsumerRecords<String, Long>> recordsList = new ArrayList<>();

        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, Long> records = consumer.poll(POLL_TIMEOUT_MS);

            LOG.info("Poll returned {} records", records.count());

            if (records.count() == 0) {
                break;
            }

            recordsList.add(records);
        }

        if (recordsList.isEmpty()) {
            return null;
        }

        return recordsList;
    }

    /* read as much as possible, and return latest record with the given key */
    private ConsumerRecord<String, Long> drain(TopicPartition partition, String key) {
        List<ConsumerRecords<String, Long>> recordsList = drain();
        if (recordsList == null) {
            throw new NoNextSekvensnummerRecordsToConsume("Did not receive any records, " +
                    "the log is empty or current position is at the end of the log");
        }

        ConsumerRecord<String, Long> recordToKeep = null;

        for (ConsumerRecords<String, Long> consumerRecords : recordsList) {
            for (ConsumerRecord<String, Long> record : consumerRecords.records(partition)) {
                if (!key.equals(record.key())) {
                    LOG.error("Log contains unexpected key {} at offset {} in {}-{}", record.key(),
                            record.offset(), record.topic(), record.partition());
                    continue;
                }
                recordToKeep = record;
            }
        }

        return recordToKeep;
    }
}
