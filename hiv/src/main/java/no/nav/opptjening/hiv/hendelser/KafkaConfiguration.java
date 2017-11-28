package no.nav.opptjening.hiv.hendelser;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import no.nav.opptjening.schema.Hendelse;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConfiguration.class);

    private final String bootstrapServers;
    private final String schemaUrl;

    private boolean initialized;
    @Value("${hiv.initialize:false}")
    private boolean shouldInitialize;

    public KafkaConfiguration(@Value("${kafka.bootstrap-servers}") String bootstrapServers,
                              @Value("${schema.registry.url}") String schemaUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaUrl = schemaUrl;
    }

    private Map<String, Object> getCommonConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return configs;
    }

    @Bean
    public SekvensnummerStorage sekvensnummerStorage(Producer<String, Long> producer, Consumer<String, Long> consumer) {
        TopicPartition partition = new TopicPartition("tortuga.inntektshendelser.offsets", 0);
        SekvensnummerStorage storage = new SekvensnummerStorage(producer, consumer, partition);

        // TODO: remove initialization
        if (shouldInitialize && !initialized) {
            LOG.info("Initializing with sekvensnummer=1");

            try {
                producer.send(new ProducerRecord<>(partition.topic(), partition.partition(), "offset", 1L));
                consumer.seekToBeginning(Collections.singletonList(partition));
                initialized = true;
            } catch (Exception e) {
                LOG.error("Error while initializing topic", e);
            }
        }

        return storage;
    }

    @Bean
    public Consumer<String, Long> offsetConsumer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "hiv-consumer-group");
        //configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-id2");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(configs);
    }

    @Bean
    public Producer<String, Long> offsetProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);

        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        return new KafkaProducer<>(configs);
    }

    @Bean
    public Producer<String, Hendelse> hendelseProducer() {
        Map<String, Object> configs = getCommonConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        return new KafkaProducer<>(configs);
    }

}
