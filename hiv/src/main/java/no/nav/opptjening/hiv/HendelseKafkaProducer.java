package no.nav.opptjening.hiv;

import io.prometheus.client.Counter;
import no.nav.opptjening.hiv.hendelser.SekvensnummerWriter;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelse;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class HendelseKafkaProducer {
    private static final Logger LOG = LoggerFactory.getLogger(HendelseKafkaProducer.class);

    private static final String BEREGNET_SKATT_HENDELSE_TOPIC = "privat-tortuga-beregnetSkattHendelseHentet";

    private static final Counter antallHendelserSendt = Counter.build()
            .name("hendelser_processed")
            .help("Antall hendelser sendt.").register();

    private static final Counter antallHendelserPersisted = Counter.build()
            .name("hendelser_persisted")
            .help("Antall hendelser bekreftet sendt.").register();

    private final Producer<String, Hendelse> producer;
    private final SekvensnummerWriter sekvensnummerWriter;

    public HendelseKafkaProducer(Producer<String, Hendelse> producer, SekvensnummerWriter sekvensnummerWriter) {
        this.producer = producer;
        this.sekvensnummerWriter = sekvensnummerWriter;
    }

    public long sendHendelser(List<Hendelse> hendelseList) {
        for (Hendelse hendelse : hendelseList) {
            ProducerRecord<String, Hendelse> record = new ProducerRecord<>(BEREGNET_SKATT_HENDELSE_TOPIC, hendelse.getGjelderPeriode() + "-" + hendelse.getIdentifikator(), hendelse);
            LOG.info("Sending record with sekvensnummer = {}", record.value().getSekvensnummer());
            producer.send(record, new ProducerCallback(producer, record, sekvensnummerWriter));
            antallHendelserSendt.inc();
        }

        return hendelseList.get(hendelseList.size() - 1).getSekvensnummer();
    }

    public void close() {
        producer.close();
    }

    private static class ProducerCallback implements Callback {
        // TODO: do they have to be volatile? callbacks are executed in the same thread, and in order?
        private static volatile int count = 0;
        private static volatile boolean shutdown = false;

        // how many records we send before persisting sekvensnummer
        private static volatile int recordsSent = 0;
        private static volatile int SEKVENSNUMMER_BUFFER = 5;

        private final Producer<String, Hendelse> producer;
        private final Thread callingThread;
        private final ProducerRecord<String, Hendelse> record;
        private final SekvensnummerWriter sekvensnummerWriter;

        private ProducerCallback(Producer<String, Hendelse> producer, ProducerRecord<String, Hendelse> record, SekvensnummerWriter sekvensnummerWriter) {
            this.producer = producer;
            this.callingThread = Thread.currentThread();
            this.record = record;
            this.sekvensnummerWriter = sekvensnummerWriter;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            count++;

            // do not persist sekvensnummer after we have begun shutdown
            if (shutdown) {
                LOG.warn("Skipping persisting of sekvensnummer = {} because we have initiated shutdown", record.value().getSekvensnummer());
                return;
            }

            if (exception != null /*|| count % 10 == 0*/) {
                LOG.error("Unrecoverable error when sending record with sekvensnummer = {}, shutting down", record.value().getSekvensnummer(), exception);
                shutdown();
            } else {
                recordsSent++;
                antallHendelserPersisted.inc();

                if (recordsSent == SEKVENSNUMMER_BUFFER) {
                    recordsSent = 0;

                    LOG.info("Record sent ok, persisting sekvensnummer = {}", record.value().getSekvensnummer());
                    try {
                        sekvensnummerWriter.writeSekvensnummer(record.value().getSekvensnummer() + 1);
                    } catch (Exception e) {
                        LOG.error("Error while writing sekvensnummer, shutting down", e);
                    } finally {
                        shutdown();
                    }
                }
            }
        }

        private void shutdown() {
            shutdown = true;
            producer.close(0, TimeUnit.MILLISECONDS);
            callingThread.interrupt();
        }
    }
}
