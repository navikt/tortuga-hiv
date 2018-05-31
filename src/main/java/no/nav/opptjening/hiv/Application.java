package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerReader;
import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerWriter;
import no.nav.opptjening.nais.ApplicationRunner;
import no.nav.opptjening.nais.NaisHttpServer;
import no.nav.opptjening.skatt.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();

        ApplicationRunner appRunner;
        try {
            NaisHttpServer naisHttpServer = new NaisHttpServer();

            KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(env);

            String hendelserUrl = env.getOrDefault("SKATT_API_URL", "https://api-gw-q0.adeo.no/ekstern/skatt/datasamarbeid/api/formueinntekt/skatteoppgjoer/");
            SkatteoppgjoerhendelserClient skatteoppgjoerhendelserClient = new SkatteoppgjoerhendelserClient(hendelserUrl, env.get("SKATT_API_KEY"));

            TopicPartition partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
            KafkaSekvensnummerReader reader = new KafkaSekvensnummerReader(kafkaConfiguration.offsetConsumer(), partition);
            KafkaSekvensnummerWriter writer = new KafkaSekvensnummerWriter(kafkaConfiguration.offsetProducer(), partition);

            SkatteoppgjorhendelsePoller poller = new SkatteoppgjorhendelsePoller(skatteoppgjoerhendelserClient, reader);
            SkatteoppgjorhendelseProducer hendelseProducer = new SkatteoppgjorhendelseProducer(kafkaConfiguration.hendelseProducer(), KafkaConfiguration.SKATTEOPPGJØRHENDELSE_TOPIC, writer);
            SkatteoppgjorhendelseTask consumer = new SkatteoppgjorhendelseTask(poller, hendelseProducer);

            appRunner = new ApplicationRunner(consumer, naisHttpServer);

            appRunner.addShutdownListener(reader::shutdown);
            appRunner.addShutdownListener(writer::shutdown);
            appRunner.addShutdownListener(hendelseProducer::shutdown);
        } catch (Exception e) {
            LOG.error("Application failed to start", e);
            return;
        }

        appRunner.run();
    }
}
