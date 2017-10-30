package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SkattConfiguration {

    @Bean
    public Hendelser hendelser(@Value("${skatt.api.url}") String baseurl, @Value("${skatt.api.hendelser-path}") String endpoint) {
        return new Hendelser(baseurl + endpoint);
    }
}
