package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.skatt.api.pgi.Inntekter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SkattConfiguration {

    @Bean
    public Inntekter hendelser(@Value("${skatt.api.url}") String baseurl, @Value("${skatt.api.pgi-path}") String endpoint) {
        return new Inntekter(baseurl + endpoint);
    }
}
