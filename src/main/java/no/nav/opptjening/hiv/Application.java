package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerReader;
import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerWriter;
import no.nav.opptjening.nais.NaisHttpServer;
import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.skatt.client.api.skatteoppgjoer.SkatteoppgjoerhendelserClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();

        final SkatteoppgjorhendelseTask app;
        try {
            final NaisHttpServer naisHttpServer = new NaisHttpServer();
            naisHttpServer.run();

            final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(env);

            String hendelserUrl = env.getOrDefault("SKATT_API_URL", "https://api-gw-q0.adeo.no/ekstern/skatt/datasamarbeid/api/formueinntekt/skatteoppgjoer/");
            final SkatteoppgjoerhendelserClient skatteoppgjoerhendelserClient = new SkatteoppgjoerhendelserClient(hendelserUrl, env.get("SKATT_API_KEY"));

            TopicPartition partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);


            Consumer<String, Long> offsetConsumer = kafkaConfiguration.offsetConsumer();
            KafkaSekvensnummerReader reader = new KafkaSekvensnummerReader(offsetConsumer, partition);

            Producer<String, Long> offsetProducer = kafkaConfiguration.offsetProducer();
            KafkaSekvensnummerWriter writer = new KafkaSekvensnummerWriter(offsetProducer, partition);

            final SkatteoppgjorhendelsePoller poller = new SkatteoppgjorhendelsePoller(skatteoppgjoerhendelserClient, reader);
            Producer<String, Hendelse> hendelseKafkaProducer = kafkaConfiguration.hendelseProducer();
            final SkatteoppgjorhendelseProducer hendelseProducer = new SkatteoppgjorhendelseProducer(hendelseKafkaProducer, KafkaConfiguration.SKATTEOPPGJØRHENDELSE_TOPIC, writer);
            app = new SkatteoppgjorhendelseTask(poller, hendelseProducer);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                offsetConsumer.close();
                offsetProducer.close();
                hendelseKafkaProducer.close();
            }));
        } catch (Exception e) {
            LOG.error("Application failed to start", e);
            System.exit(1);
            return;
        }

        app.run();
    }
}
