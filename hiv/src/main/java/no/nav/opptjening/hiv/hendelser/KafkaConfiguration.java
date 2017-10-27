package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.schema.InntektHendelse;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfiguration {

    @Bean
    public ProducerFactory<String, InntektHendelse> producerFactory(KafkaProperties properties) {
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, InntektHendelse> kafkaTemplate(ProducerFactory<String, InntektHendelse> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}