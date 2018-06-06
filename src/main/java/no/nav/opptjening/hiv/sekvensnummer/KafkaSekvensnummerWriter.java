package no.nav.opptjening.hiv.sekvensnummer;

import io.prometheus.client.Gauge;
import no.nav.opptjening.nais.signals.Signaller;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSekvensnummerWriter implements SekvensnummerWriter {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSekvensnummerWriter.class);

    static final String NEXT_SEKVENSNUMMER_KEY = "nextSekvensnummer";

    private final Producer<String, Long> producer;
    private final TopicPartition topicPartition;

    private static final Gauge nextSekvensnummerGauge = Gauge.build()
            .name("next_sekvensnummer_written")
            .help("Det siste 'neste sekvensnummer' vi forventer å få hendelser på, som er skrevet til Kafka").register();
    private static final Gauge nextSekvensnummerOffsetGauge = Gauge.build()
            .name("next_sekvensnummer_written_offset")
            .help("Offset til det siste 'neste sekvensnummer' vi forventer å få hendelser på, som er skrevet til Kafka").register();

    private final Signaller.CallbackSignaller shutdownSignal = new Signaller.CallbackSignaller();

    public KafkaSekvensnummerWriter(@NotNull Producer<String, Long> producer, @NotNull TopicPartition topicPartition) {
        this.producer = producer;
        this.topicPartition = topicPartition;
        shutdownSignal.addListener(() -> {
            LOG.error("Received shutdown signal. Shutting down.");
            shutdown();
        });
    }

    public void shutdown() {
        LOG.info("Shutting down KafkaSekvensnummerWriter");
        producer.close();
    }

    public void writeSekvensnummer(long sekvensnummer) {
        LOG.info("Writing sekvensnummer={}", sekvensnummer);

        ProducerRecord<String, Long> record = new ProducerRecord<>(topicPartition.topic(),
                topicPartition.partition(), NEXT_SEKVENSNUMMER_KEY, sekvensnummer);
        producer.send(record, new ProducerCallback(record, shutdownSignal));
    }

    private static class ProducerCallback implements Callback {
        private final ProducerRecord<String, Long> record;
        private final Signaller shutdownSignal;

        private ProducerCallback(@NotNull ProducerRecord<String, Long> record, @NotNull Signaller shutdownSignal) {
            this.record = record;
            this.shutdownSignal = shutdownSignal;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (shutdownSignal.signalled()) {
                return;
            }

            if (e != null) {
                LOG.error("Error while sending sekvensnummer={}. Signalling shutdown.", record.value(), e);
                shutdownSignal.signal();
            } else {
                LOG.info("Sekvensnummer={} sent with offset = {}", record.value(), recordMetadata.offset());
                nextSekvensnummerOffsetGauge.set(recordMetadata.offset());
                nextSekvensnummerGauge.set(record.value());
            }
        }
    }
}
