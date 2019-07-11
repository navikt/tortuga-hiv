package no.nav.opptjening.hiv.testsupport;

import no.nav.opptjening.hiv.KafkaConfiguration;

import java.util.Map;

public class PlainTextSecurityConfig implements KafkaConfiguration.KafkaSecurtyConfig {
    @Override
    public Map<String, Object> getSecurityConfig() {
        return Map.of(KafkaConfiguration.KafkaSecurtyConfig.Properties.SECURITY_PROTOCOL, "PLAINTEXT");
    }
}