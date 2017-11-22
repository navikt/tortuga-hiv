package no.nav.opptjening.hoi.hendelser;

import no.nav.opptjening.skatt.api.pgi.Inntekter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SkattConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(SkattConfiguration.class);

    @Bean
    public Inntekter hendelser(@Value("${skatt.api.url}") String baseurl) {
        LOG.info("Creating Inntekter bean with baseurl={}", baseurl);
        return new Inntekter(baseurl);
    }
}
