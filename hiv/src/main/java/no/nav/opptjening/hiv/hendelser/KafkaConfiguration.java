package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.Hendelse;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.*;

import java.util.Map;

@Configuration
public class KafkaConfiguration {

    private final KafkaProperties properties;

    public KafkaConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    public KafkaTemplate<String, Hendelse> kafkaHendelseTemplate(
            ProducerFactory<String, Hendelse> kafkaProducerFactory) {
        KafkaTemplate<String, Hendelse> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());

        return kafkaTemplate;
    }

    @Bean
    public KafkaTemplate<String, Long> kafkaHendelseOffsetTemplate(
            ProducerFactory<String, Long> kafkaProducerFactory) {
        KafkaTemplate<String, Long> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());

        return kafkaTemplate;
    }

    @Bean
    public ConsumerFactory<Object, Object> kafkaConsumerFactory() {
        Map<String, Object> props = properties.buildConsumerProperties();
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConsumerFactory<String, Long> kafkaOffsetConsumerFactory() {
        Map<String, Object> props = properties.buildConsumerProperties();
        props.remove("schema.registry.url");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ProducerFactory<String, Long> kafkaHendelseOffsetProducerFactory() {
        Map<String, Object> props = this.properties.buildProducerProperties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.remove("schema.registry.url");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ProducerFactory<String, Hendelse> kafkaHendelseProducerFactory() {
        Map<String, Object> props = this.properties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(props);
    }
}
