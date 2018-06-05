package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerWriter;
import no.nav.opptjening.nais.signals.Signaller;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelsesliste;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SkatteoppgjorhendelseProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelseProducer.class);

    private static final Counter antallHendelserSendt = Counter.build()
            .name("hendelser_processed")
            .help("Antall hendelser sendt.").register();

    private static final Counter antallHendelserPersisted = Counter.build()
            .name("hendelser_persisted")
            .help("Antall hendelser bekreftet sendt.").register();

    private final Producer<String, Hendelsesliste.Hendelse> producer;
    private final String topic;
    private final SekvensnummerWriter sekvensnummerWriter;

    private final Signaller.CallbackSignaller shutdownSignal = new Signaller.CallbackSignaller();

    public SkatteoppgjorhendelseProducer(Producer<String, Hendelsesliste.Hendelse> producer, String topic, SekvensnummerWriter sekvensnummerWriter) {
        this.producer = producer;
        this.topic = topic;
        this.sekvensnummerWriter = sekvensnummerWriter;
        this.shutdownSignal.addListener(() -> {
            LOG.info("Shutting signalled received, shutting down hendelse producer");
            shutdown();
        });
    }

    public long sendHendelser(List<Hendelsesliste.Hendelse> hendelseList) {
        for (Hendelsesliste.Hendelse hendelse : hendelseList) {
            ProducerRecord<String, Hendelsesliste.Hendelse> record = new ProducerRecord<>(topic, hendelse.getGjelderPeriode() + "-" + hendelse.getIdentifikator(), hendelse);
            LOG.info("Sending record with sekvensnummer = {}", record.value().getSekvensnummer());
            producer.send(record, new ProducerCallback(record, sekvensnummerWriter, shutdownSignal));
            antallHendelserSendt.inc();
        }

        return hendelseList.get(hendelseList.size() - 1).getSekvensnummer();
    }

    public void shutdown() {
        LOG.info("Shutting down SkatteoppgjorhendelseProducer");
        producer.close();
    }

    private static class ProducerCallback implements Callback {
        private final ProducerRecord<String, Hendelsesliste.Hendelse> record;
        private final SekvensnummerWriter sekvensnummerWriter;
        private final Signaller shutdownSignal;

        private ProducerCallback(ProducerRecord<String, Hendelsesliste.Hendelse> record, SekvensnummerWriter sekvensnummerWriter, Signaller shutdownSignal) {
            this.record = record;
            this.sekvensnummerWriter = sekvensnummerWriter;
            this.shutdownSignal = shutdownSignal;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            // do not persist sekvensnummer after we have begun shutdown
            if (shutdownSignal.signalled()) {
                LOG.warn("Skipping persisting of sekvensnummer = {} because we have initiated shutdown", record.value().getSekvensnummer());
                return;
            }

            if (exception != null) {
                LOG.error("Unrecoverable error when sending record with sekvensnummer = {}, shutting down", record.value().getSekvensnummer(), exception);
                shutdownSignal.signal();
            } else {
                antallHendelserPersisted.inc();

                LOG.info("Record sent ok, persisting sekvensnummer = {}", record.value().getSekvensnummer());
                try {
                    sekvensnummerWriter.writeSekvensnummer(record.value().getSekvensnummer() + 1);
                } catch (Exception e) {
                    LOG.error("Error while writing sekvensnummer, shutting down", e);
                    shutdownSignal.signal();
                }
            }
        }
    }
}
