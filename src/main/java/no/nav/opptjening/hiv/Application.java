package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerReader;
import no.nav.opptjening.hiv.sekvensnummer.KafkaSekvensnummerWriter;
import no.nav.opptjening.skatt.api.beregnetskatt.BeregnetSkattHendelserClient;
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

            BeregnetSkattHendelserClient beregnetSkattHendelserClient = new BeregnetSkattHendelserClient(env.getOrDefault("SKATT_API_URL", "http://tortuga-testapi/ekstern/skatt/datasamarbeid/api/formueinntekt/skatteoppegjoer/"));
            TopicPartition partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
            KafkaSekvensnummerReader reader = new KafkaSekvensnummerReader(kafkaConfiguration.offsetConsumer(), partition);
            KafkaSekvensnummerWriter writer = new KafkaSekvensnummerWriter(kafkaConfiguration.offsetProducer(), partition);

            SkatteoppgjorhendelsePoller poller = new SkatteoppgjorhendelsePoller(beregnetSkattHendelserClient, reader);
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
