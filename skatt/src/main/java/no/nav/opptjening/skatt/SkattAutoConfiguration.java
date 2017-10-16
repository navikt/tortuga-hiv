package no.nav.opptjening.skatt;

import no.nav.opptjening.skatt.api.InntektHendelser;
import no.nav.opptjening.skatt.api.Inntekter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SkattProperties.class)
public class SkattAutoConfiguration {

    private final SkattProperties properties;

    public SkattAutoConfiguration(SkattProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean
    public InntektHendelser inntektHendelser() {
        return new InntektHendelser(properties.getHendelserUrl());
    }

    @Bean
    @ConditionalOnMissingBean
    public Inntekter inntekter() {
        return new Inntekter(properties.getPgiUrl());
    }
}
