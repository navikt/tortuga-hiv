package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.CouldNotFindNextSekvensnummerRecord;
import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerReader;
import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerWriter;
import no.nav.opptjening.nais.NaisHttpServer;
import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;
import no.nav.opptjening.skatt.client.Hendelsesliste;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import no.nav.opptjening.skatt.client.exceptions.HttpException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.SocketTimeoutException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    private static final int POLL_TIMEOUT_MS = 5000;
    private static final int AMOUNT_OF_HENDELSER_PER_REQUEST = 1000;

    private final SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller;
    private final SkatteoppgjorhendelseProducer hendelseProducer;

    private final HendelseMapper hendelseMapper = new HendelseMapper();

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();

        final Application app;
        try {
            final NaisHttpServer naisHttpServer = new NaisHttpServer();
            naisHttpServer.start();

            final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(env);

            String hendelserUrl = env.get("SKATT_API_URL");
            String skattApiKey = env.get("SKATT_API_KEY");
            String earliestValidHendelseYear = env.get("EARLIEST_VALID_HENDELSE_YEAR");

            final SkatteoppgjoerhendelserClient skatteoppgjoerhendelserClient = new SkatteoppgjoerhendelserClient(hendelserUrl, skattApiKey);

            TopicPartition partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);

            Consumer<String, Long> offsetConsumer = kafkaConfiguration.offsetConsumer();
            KafkaSekvensnummerReader reader = new KafkaSekvensnummerReader(offsetConsumer, partition);

            Producer<String, Long> offsetProducer = kafkaConfiguration.offsetProducer();
            KafkaSekvensnummerWriter writer = new KafkaSekvensnummerWriter(offsetProducer, partition);

            final SkatteoppgjorhendelsePoller poller = new SkatteoppgjorhendelsePoller(skatteoppgjoerhendelserClient, reader, LocalDate::now, AMOUNT_OF_HENDELSER_PER_REQUEST);
            Producer<HendelseKey, Hendelse> hendelseKafkaProducer = kafkaConfiguration.hendelseProducer();
            final SkatteoppgjorhendelseProducer hendelseProducer = new SkatteoppgjorhendelseProducer(hendelseKafkaProducer, KafkaConfiguration.SKATTEOPPGJØRHENDELSE_TOPIC, writer, earliestValidHendelseYear);
            app = new Application(poller, hendelseProducer);

            addShutdownHook(offsetConsumer, offsetProducer, hendelseKafkaProducer);
        } catch (Exception e) {
            LOG.error("Application failed to start", e);
            System.exit(1);
            return;
        }

        app.run();
        System.exit(0);
    }


    private Application(SkatteoppgjorhendelsePoller skatteoppgjorhendelsePoller, SkatteoppgjorhendelseProducer hendelseProducer) {
        this.skatteoppgjorhendelsePoller = skatteoppgjorhendelsePoller;
        this.hendelseProducer = hendelseProducer;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private void run() {
        try {
            while (true) {
                try {
                    MDC.put("requestId", UUID.randomUUID().toString());
                    List<Hendelsesliste.Hendelse> hendelsesliste = skatteoppgjorhendelsePoller.poll();

                    List<Hendelse> internalHendelser = hendelsesliste.stream()
                            .map(hendelseMapper::mapToInternalHendelse)
                            .collect(Collectors.toList());

                    hendelseProducer.sendHendelser(internalHendelser);
                } catch (EmptyResultException e) {
                    LOG.debug("Skatteetaten reported no new records, waiting a bit before trying again", e);
                    Thread.sleep(POLL_TIMEOUT_MS);
                } catch (SocketTimeoutException e) {
                    LOG.debug("Socket timeout, waiting a bit before trying again", e);
                    Thread.sleep(POLL_TIMEOUT_MS);
                } catch (HttpException e) {
                    LOG.error("Error while contacting Skatteetaten", e);
                    Thread.sleep(POLL_TIMEOUT_MS);
                } finally {
                    MDC.remove("requestId");
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Thread got interrupted during sleep, exiting", e);
        } catch (CouldNotFindNextSekvensnummerRecord e) {
            LOG.error(e.getMessage(), e);
        } catch (KafkaException e) {
            LOG.error("Error while consuming or producing data on Kafka", e);
        } catch (Error | Exception e) {
            LOG.error("Unknown error", e);
        }

        LOG.info("Skatteetaten task stopped");
    }

    private static void addShutdownHook(Consumer<String, Long> offsetConsumer, Producer<String, Long> offsetProducer, Producer<HendelseKey, Hendelse> hendelseKafkaProducer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            offsetConsumer.close();
            offsetProducer.close();
            hendelseKafkaProducer.close();
        }));
    }
}
