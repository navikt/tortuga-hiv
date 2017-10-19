package no.nav.opptjening.skatt;

import no.nav.opptjening.skatt.api.InntektHendelser;
import no.nav.opptjening.skatt.api.Inntekter;
import no.nav.opptjening.skatt.api.SkattErrorHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableConfigurationProperties(SkattProperties.class)
public class SkattAutoConfiguration {

    private final SkattProperties properties;

    public SkattAutoConfiguration(SkattProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean
    public SkattErrorHandler skattErrorHandler() {
        return new SkattErrorHandler();
    }

    @Bean
    @ConditionalOnMissingBean
    public RestTemplate skattRestTemplate(SkattErrorHandler errorHandler) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(errorHandler);

        return restTemplate;
    }

    @Bean
    @ConditionalOnMissingBean
    public InntektHendelser inntektHendelser(RestTemplate restTemplate) {
        return new InntektHendelser(properties.getHendelserUrl(), restTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public Inntekter inntekter(RestTemplate restTemplate) {
        return new Inntekter(properties.getPgiUrl(), restTemplate);
    }
}
