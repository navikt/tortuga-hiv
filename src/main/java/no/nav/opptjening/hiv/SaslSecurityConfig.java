package no.nav.opptjening.hiv;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.HashMap;
import java.util.Map;

public class SaslSecurityConfig implements KafkaConfiguration.KafkaSecurtyConfig {

        class Properties {
            static final String USERNAME = "KAFKA_USERNAME";
            static final String PASSWORD = "KAFKA_PASSWORD";
        }
        private final String saslJaasConfig;

        SaslSecurityConfig(Map<String, String> env) {
            this.saslJaasConfig = createPlainLoginModule(env.get(Properties.USERNAME), env.get(Properties.PASSWORD));
        }
        
        private String createPlainLoginModule(String username, String password) {
            return String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", username, password);
        }

        @Override
        public Map<String, Object> getSecurityConfig() {
            Map<String, Object> configs = new HashMap<>();
            configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            configs.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
            return configs;
        }
    }