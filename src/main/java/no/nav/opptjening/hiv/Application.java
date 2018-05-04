package no.nav.opptjening.hiv;

import no.nav.opptjening.hiv.hendelser.KafkaSekvensnummerReader;
import no.nav.opptjening.hiv.hendelser.KafkaSekvensnummerWriter;
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

            BeregnetSkattHendelserClient beregnetSkattHendelserClient = new BeregnetSkattHendelserClient(env.getOrDefault("SKATT_API_URL", "http://tortuga-testapi/ekstern/skatt/datasamarbeid/api/formueinntekt/beregnetskatt/"));
            TopicPartition partition = new TopicPartition(KafkaConfiguration.SEKVENSNUMMER_TOPIC, 0);
            KafkaSekvensnummerReader reader = new KafkaSekvensnummerReader(kafkaConfiguration.offsetConsumer(), partition);
            KafkaSekvensnummerWriter writer = new KafkaSekvensnummerWriter(kafkaConfiguration.offsetProducer(), partition);
            HendelseKafkaProducer hendelseProducer = new HendelseKafkaProducer(kafkaConfiguration.hendelseProducer(), writer);
            BeregnetSkattHendelserConsumer consumer = new BeregnetSkattHendelserConsumer(beregnetSkattHendelserClient, hendelseProducer, reader);

            appRunner = new ApplicationRunner(consumer, naisHttpServer);
        } catch (Exception e) {
            LOG.error("Application failed to start", e);
            return;
        }

        appRunner.run();
    }
}
