package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.dto.InntektKafkaHendelseDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class InntektHendelseProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private KafkaProperties kafkaProperties;

    private static final Logger LOG = LoggerFactory.getLogger(InntektHendelseProducerConfig.class);

    public InntektHendelseProducerConfig(KafkaProperties properties) {
        this.kafkaProperties = properties;
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        return kafkaProperties.buildProducerProperties();
    }

    @Bean
    public ProducerFactory<String, InntektKafkaHendelseDto> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, InntektKafkaHendelseDto> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}