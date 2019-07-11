package no.nav.opptjening.hiv;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class SaslSecurityConfig implements KafkaConfiguration.KafkaSecurtyConfig {

        class Properties {
            static final String USERNAME = "KAFKA_USERNAME";
            static final String PASSWORD = "KAFKA_PASSWORD";
            static final String TRUSTSTORE_LOCATION = "KAFKA_SSL_TRUSTSTORE_LOCATION";
            static final String TRUSTSTORE_PASSWORD = "KAFKA_SSL_TRUSTSTORE_PASSWORD";
        }

        private final File truststoreLocation;
        private final String truststorePassword;
        private final String saslJaasConfig;

        public SaslSecurityConfig(Map<String, String> env) {
            this.saslJaasConfig = createPlainLoginModule(env.get(Properties.USERNAME), env.get(Properties.PASSWORD));
            this.truststoreLocation = resourceToFile(env.get(Properties.TRUSTSTORE_LOCATION));
            this.truststorePassword = env.get(Properties.TRUSTSTORE_PASSWORD);
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
            configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation.getAbsolutePath());
            configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
            
            return configs;
        }

        private File resourceToFile(String path) {
            var resourceUrl = KafkaConfiguration.class.getClassLoader().getResource(path);
            if (resourceUrl == null) throw new RuntimeException("Invalid truststore file: Resource " + path + " can not be found, or insufficient privileges");

            return new File(resourceUrl.getFile());
        }
    }