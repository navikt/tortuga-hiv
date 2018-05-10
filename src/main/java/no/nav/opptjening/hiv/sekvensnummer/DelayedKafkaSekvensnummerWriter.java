package no.nav.opptjening.hiv.sekvensnummer;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class DelayedKafkaSekvensnummerWriter implements SekvensnummerWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedKafkaSekvensnummerWriter.class);

    static final String NEXT_SEKVENSNUMMER_KEY = "nextSekvensnummer";

    private final Producer<String, Long> producer;
    private final TopicPartition topicPartition;

    private static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer_written")
            .help("Det siste 'neste sekvensnummer' vi forventer å få hendelser på, som er skrevet til Kafka").register();
    private static final Gauge nextSekvensnummerOffsetGauge = Gauge.build()
            .name("next_sekvensnummer_written_offset")
            .help("Offset til det siste 'neste sekvensnummer' vi forventer å få hendelser på, som er skrevet til Kafka").register();


    private final Object sekvensnummerLock = new Object();
    private volatile long nextSekvensnummer;
    private volatile boolean shutdown;

    public DelayedKafkaSekvensnummerWriter(Producer<String, Long> producer, TopicPartition topicPartition) {
        this.producer = producer;
        this.topicPartition = topicPartition;

        new Thread(() -> {
            synchronized (sekvensnummerLock) {
                if (nextSekvensnummer == 0) {
                    return;
                }

                ProducerRecord<String, Long> record = new ProducerRecord<>(topicPartition.topic(),
                        topicPartition.partition(), NEXT_SEKVENSNUMMER_KEY, nextSekvensnummer);

                nextSekvensnummer = 0;

                // TODO: don't mix the synchronized block above and a syncronous kafka call,
                // we will end up holding the sekvensnummerLock for too long
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    RecordMetadata recordMetadata = future.get();

                    LOG.info("Sekvensnummer={} sent with offset = {}", record.value(), recordMetadata.offset());
                    nextSekvensnummerOffsetGauge.set(recordMetadata.offset());
                    nextSekvensnummerGauge.set(record.value());
                } catch (Exception e) {
                    LOG.error("Error while sending sekvensnummer={}. Shutting down.", record.value(), e);
                    producer.close(0, TimeUnit.MILLISECONDS);
                }
            }
        });
    }

    public void writeSekvensnummer(long sekvensnummer) {
        if (shutdown) {
            throw new RuntimeException("Producer shut down");
        }

        LOG.info("Writing sekvensnummer={}", sekvensnummer);
        synchronized (sekvensnummerLock) {
            nextSekvensnummer = sekvensnummer;
        }
    }
}
