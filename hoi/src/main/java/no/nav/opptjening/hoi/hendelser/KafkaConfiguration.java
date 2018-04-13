package no.nav.opptjening.hoi.hendelser;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import no.nav.opptjening.schema.PensjonsgivendeInntekt;
import no.nav.opptjening.schema.skatteetaten.hendelsesliste.Hendelse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    public static final String BEREGNET_SKATT_HENDELSE_TOPIC = "privat-tortuga-beregnetSkattHendelseHentet";
    public static final String PENSJONSGIVENDE_INNTEKT_TOPIC = "aapen-opptjening-pensjonsgivendeInntekt";
    private final String bootstrapServers;
    private final String schemaUrl;

    private String securityProtocol;
    private Resource truststoreLocation;
    private String truststorePassword;

    private String saslMechanism;

    private String saslJaasConfig;

    public KafkaConfiguration(@Value("${kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${schema.registry.url}") String schemaUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaUrl = schemaUrl;
    }

    @Value("${kafka.sasl.jaas.config:#{null}}")
    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    @Value("${kafka.sasl.mechanism:#{null}}")
    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    @Value("${kafka.security.protocol:#{null}}")
    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    @Value("${kafka.ssl.truststore.location:#{null}}")
    public void setTruststoreLocation(Resource truststoreLocation) {
        this.truststoreLocation = truststoreLocation;
    }

    @Value("${kafka.ssl.truststore.password:#{null}}")
    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    private static String resourceToPath(Resource resource) {
        try {
            return resource.getFile().getAbsolutePath();
        } catch (IOException ex) {
            throw new IllegalStateException("Resource '" + resource + "' must be on a file system", ex);
        }
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
            configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, resourceToPath(truststoreLocation));
        }
        if (truststorePassword != null) {
            configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        }

        return configs;
    }

    @Bean
    public Consumer<String, Hendelse> hendelseConsumer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        configs.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "hoi-consumer-group");
        //configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-id2");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        return new KafkaConsumer<>(configs);
    }

    @Bean
    public Producer<String, PensjonsgivendeInntekt> inntektsProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        return new KafkaProducer<>(configs);
    }

}
