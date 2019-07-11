package no.nav.opptjening.hiv;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import no.nav.opptjening.schema.skatt.hendelsesliste.Hendelse;
import no.nav.opptjening.schema.skatt.hendelsesliste.HendelseKey;
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
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static no.nav.opptjening.hiv.KafkaConfiguration.KafkaSecurtyConfig.Properties.SECURITY_PROTOCOL;

public class KafkaConfiguration {

    public static final String SKATTEOPPGJORHENDELSE_TOPIC = "privat-tortuga-skatteoppgjorhendelse";
    public static final String SEKVENSNUMMER_TOPIC = "privat-tortuga-sekvensnummerTilstand";

    public static class Properties {
        public static final String BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
        public static final String SCHEMA_REGISTRY_URL = "SCHEMA_REGISTRY_URL";
    }

    private final String bootstrapServers;
    private final String schemaUrl;

    private KafkaSecurtyConfig securtyConfig;

    public KafkaConfiguration(Map<String, String> env, Function<Map<String, String>, KafkaSecurtyConfig> securityStrategy) {
        this.bootstrapServers = Objects.requireNonNull(env.get(Properties.BOOTSTRAP_SERVERS));
        this.schemaUrl = env.getOrDefault(Properties.SCHEMA_REGISTRY_URL, "http://kafka-schema-registry.tpa:8081");
        securtyConfig = securityStrategy.apply(env);
    }

    public Consumer<String, Long> offsetConsumer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "hiv-consumer-group");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(configs);
    }

    public Producer<String, Long> offsetProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        configs.put(ProducerConfig.RETRIES_CONFIG, 0);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        return new KafkaProducer<>(configs);
    }

    Producer<HendelseKey, Hendelse> hendelseProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        return new KafkaProducer<>(configs);
    }

    private Map<String, Object> getCommonConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.putAll(securtyConfig.getSecurityConfig());
        return configs;
    }

    public interface KafkaSecurtyConfig {
        class Properties {
            public static final String SECURITY_PROTOCOL = "KAFKA_SECURITY_PROTOCOL";
        }
        Map<String, Object> getSecurityConfig();
    }
}
