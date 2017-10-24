package no.nav.opptjening.hiv.hendelser.support;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchLoggConfiguration {
    @Bean
    BatchLoggService batchLoggService(BatchLoggRepository repository) {
        return new BatchLoggService(repository);
    }
}
