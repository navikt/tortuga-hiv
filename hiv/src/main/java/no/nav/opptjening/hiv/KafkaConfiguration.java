package no.nav.opptjening.hiv;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import no.nav.opptjening.skatt.schema.hendelsesliste.Hendelse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class KafkaConfiguration {

    public static final String SEKVENSNUMMER_TOPIC = "privat-tortuga-sekvensnummerTilstand";

    private final String bootstrapServers;
    private final String schemaUrl;
    private final String securityProtocol;
    private final File truststoreLocation;
    private final String truststorePassword;
    private final String saslMechanism;
    private final String saslJaasConfig;
    private final String username;
    private final String password;

    public KafkaConfiguration(Map<String, String> env) {
        this.bootstrapServers = env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "b27apvl00045.preprod.local:8443,b27apvl00046.preprod.local:8443,b27apvl00047.preprod.local:8443");
        this.schemaUrl = env.getOrDefault("SCHEMA_REGISTRY_URL", "http://tpa-confluent-nais-confluent-schema-registry.tpa:8081");

        this.username = env.get("KAFKA_USERNAME");
        this.password = env.get("KAFKA_PASSWORD");

        this.saslJaasConfig = nullIfEmpty(env.getOrDefault("KAFKA_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";"));
        this.saslMechanism = nullIfEmpty(env.getOrDefault("KAFKA_SASL_MECHANISM", "PLAIN"));
        this.securityProtocol = nullIfEmpty(env.getOrDefault("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"));

        try {
            this.truststoreLocation = resourceToFile(env.get("KAFKA_SSL_TRUSTSTORE_LOCATION"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Invalid truststore file", e);
        }

        this.truststorePassword = nullIfEmpty(env.get("KAFKA_SSL_TRUSTSTORE_PASSWORD"));
    }

    private static String nullIfEmpty(String value) {
        if ("".equals(value)) {
            return null;
        }
        return value;
    }

    public Consumer<String, Long> offsetConsumer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "hiv-consumer-group1");
        //configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-id2");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        //return new MockConsumer<>(null);
        return new KafkaConsumer<>(configs);
    }

    public Producer<String, Long> offsetProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        //return new MockProducer<>();
        return new KafkaProducer<>(configs);
    }

    public Producer<String, Hendelse> hendelseProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        //return new MockProducer<>();
        return new KafkaProducer<>(configs);
    }

    private Map<String, Object> getCommonConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        if (securityProtocol != null) {
            configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }

        if (saslMechanism != null) {
            configs.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }

        if (saslJaasConfig != null) {
            configs.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }

        if (truststoreLocation != null) {
            configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation.getAbsolutePath());
        }
        if (truststorePassword != null) {
            configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        }

        return configs;
    }

    private static File resourceToFile(String path) throws FileNotFoundException {
        if (path == null) {
            return null;
        }

        ClassLoader classLoader = KafkaConfiguration.class.getClassLoader();
        URL resourceUrl = classLoader.getResource(path);

        if (resourceUrl == null) {
            throw new FileNotFoundException("Resource " + path + " can not be found, or insufficient privileges");
        }

        return new File(resourceUrl.getFile());
    }
}
