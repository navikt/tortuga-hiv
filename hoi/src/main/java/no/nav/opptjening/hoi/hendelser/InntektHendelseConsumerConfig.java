package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.dto.InntektKafkaHendelseDto;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
@EnableKafka
public class InntektHendelseConsumerConfig {

    private KafkaProperties kafkaProperties;

    public InntektHendelseConsumerConfig(KafkaProperties properties) {
        this.kafkaProperties = properties;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        return kafkaProperties.buildConsumerProperties();
    }

    @Bean
    public ConsumerFactory<String, InntektKafkaHendelseDto> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
                new JsonDeserializer<>(InntektKafkaHendelseDto.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InntektKafkaHendelseDto> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, InntektKafkaHendelseDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}