package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.skatt.api.hendelser.Hendelser;
import no.nav.opptjening.skatt.api.pgi.InntektHendelser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SkattConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(SkattConfiguration.class);

    @Bean
    public Hendelser hendelser(@Value("${skatt.api.url}") String baseurl) {
        LOG.info("Creating Hendelser bean with baseurl={}", baseurl);
        return new InntektHendelser(baseurl);
    }
}
