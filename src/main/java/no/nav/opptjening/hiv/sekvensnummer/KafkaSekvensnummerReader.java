package no.nav.opptjening.hiv.sekvensnummer;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class KafkaSekvensnummerReader implements SekvensnummerReader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSekvensnummerReader.class);

    private static final int POLL_TIMEOUT_MS = 1000;

    static final String NEXT_SEKVENSNUMMER_KEY = "nextSekvensnummer";

    private final Consumer<String, Long> consumer;
    private final TopicPartition topicPartition;

    private static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer_persisted")
            .help("Neste sekvensnummer vi forventer å få hendelser på, som er lagret på Kafka").register();
    private static final Gauge nextSekvensnummerOffsetGauge = Gauge.build()
            .name("next_sekvensnummer_persisted_offset")
            .help("Offset til neste sekvensnummer vi forventer å få hendelser på, som er lagret på Kafka").register();

    public KafkaSekvensnummerReader(@NotNull Consumer<String, Long> consumer, @NotNull TopicPartition topicPartition) {
        this.consumer = consumer;
        this.topicPartition = topicPartition;
    }

    public Optional<Long> readSekvensnummer() {
        consumer.assign(Collections.singletonList(topicPartition));

        OffsetAndMetadata committedOffset = consumer.committed(topicPartition);
        LOG.info("Committed offset = {}", committedOffset);

        if (committedOffset == null) {
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

            OffsetAndMetadata offsetToCommit = new OffsetAndMetadata(nextSekvensnummer.offset() + 1);
            consumer.commitSync(Collections.singletonMap(topicPartition, offsetToCommit));
            LOG.info("Offset={} committed (committed offset = {})", offsetToCommit.offset(),
                    consumer.committed(topicPartition).offset());

            nextSekvensnummerOffsetGauge.set(nextSekvensnummer.offset());
            nextSekvensnummerGauge.set(nextSekvensnummer.value());

            return Optional.of(nextSekvensnummer.value());
        } catch (NoNextSekvensnummerRecordsToConsume e) {
            if (committedOffset == null) {
                LOG.info("No records were consumed, and committed offset was null, so we assume the log is empty");
                return Optional.empty();
            }

            throw e;
        } finally {
            consumer.unsubscribe();
        }
    }

    /* read as much as possible, and return latest record with the given key */
    @Nullable
    private ConsumerRecord<String, Long> drain(@NotNull TopicPartition partition, @NotNull String key) {
        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

        if (records.count() == 0) {
            throw new NoNextSekvensnummerRecordsToConsume("Did not receive any records, " +
                    "the log is empty or current position is at the end of the log");
        }

        ConsumerRecord<String, Long> recordToKeep = null;
        while (!Thread.currentThread().isInterrupted() && records.count() > 0) {
            LOG.info("Poll returned {} records", records.count());

            for (ConsumerRecord<String, Long> record : records.records(partition)) {
                if (!key.equals(record.key())) {
                    LOG.warn("Log contains unexpected key {} at offset {} in {}-{}", record.key(),
                            record.offset(), record.topic(), record.partition());
                    continue;
                }
                recordToKeep = record;
            }

            records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
        }

        return recordToKeep;
    }
}
