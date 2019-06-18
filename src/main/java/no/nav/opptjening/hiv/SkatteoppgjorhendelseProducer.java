package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import no.nav.opptjening.hiv.sekvensnummer.SekvensnummerWriter;
import no.nav.opptjening.nais.signals.Signaller;
import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class SkatteoppgjorhendelseProducer {
    private static final Logger LOG = LoggerFactory.getLogger(SkatteoppgjorhendelseProducer.class);

    private static final Counter antallHendelserSendt = Counter.build()
            .name("hendelser_processed")
            .help("Antall hendelser sendt.").register();

    private static final Counter antallHendelserPersisted = Counter.build()
            .name("hendelser_persisted")
            .help("Antall hendelser bekreftet sendt.").register();

    private final Producer<HendelseKey, Hendelse> producer;
    private final String topic;
    private final SekvensnummerWriter sekvensnummerWriter;

    private final Signaller.CallbackSignaller shutdownSignal = new Signaller.CallbackSignaller();

    private final HendelseProducerRecordMapper hendelseProducerRecordMapper = new HendelseProducerRecordMapper();
    private final int earliestValidHendelseYear;

    public SkatteoppgjorhendelseProducer(Producer<HendelseKey, Hendelse> producer, String topic, SekvensnummerWriter sekvensnummerWriter, String earliestValidHendelseYear) {
        this.producer = producer;
        this.topic = topic;
        this.sekvensnummerWriter = sekvensnummerWriter;
        this.earliestValidHendelseYear = Integer.parseInt(earliestValidHendelseYear);
        this.shutdownSignal.addListener(shutdownListener());
    }

    private Signaller.SignalListener shutdownListener() {
        return () -> {
            LOG.info("Shutting signal received, shutting down hendelse producer");
            shutdown();
        };
    }

    public long sendHendelser(List<Hendelse> hendelseList) {
        List<Hendelse> hendelser = hendelseList.stream()
                .filter(hendelse -> Integer.parseInt(hendelse.getGjelderPeriode()) >= earliestValidHendelseYear)
                .collect(Collectors.toList());

        hendelser.stream()
                .map((hendelse) -> hendelseProducerRecordMapper.mapToProducerRecord(topic, hendelse))
                .forEach((record) -> {
                    producer.send(record, new ProducerCallback(record, sekvensnummerWriter, shutdownSignal));
                    antallHendelserSendt.inc();
                });

        long sisteSekvensnummer = hendelseList.get(hendelseList.size() - 1).getSekvensnummer();

        if (hendelser.size() == 0) {
            LOG.info("No hendelser to send after filtering away unwanted hendelser");
            sekvensnummerWriter.writeSekvensnummer(sisteSekvensnummer + 1);
        }

        return sisteSekvensnummer;
    }

    public void shutdown() {
        LOG.info("Shutting down SkatteoppgjorhendelseProducer");
        producer.close();
    }

    private static class ProducerCallback implements Callback {
        private final ProducerRecord<HendelseKey, Hendelse> record;
        private final SekvensnummerWriter sekvensnummerWriter;
        private final Signaller shutdownSignal;

        private ProducerCallback(ProducerRecord<HendelseKey, Hendelse> record, SekvensnummerWriter sekvensnummerWriter, Signaller shutdownSignal) {
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

                LOG.trace("Record sent ok, persisting sekvensnummer = {}", record.value().getSekvensnummer());
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
