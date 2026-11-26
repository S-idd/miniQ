package dev.miniq.spring;

import dev.miniq.client.MiniQClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Registers a lazily connecting {@link MiniQConnectionManager} and a {@link MiniQTemplate}. */
@AutoConfiguration
@ConditionalOnClass(MiniQClient.class)
@ConditionalOnProperty(prefix = "miniq", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(MiniQProperties.class)
public class MiniQAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public MiniQConnectionManager miniQConnectionManager(MiniQProperties properties) {
        return new MiniQConnectionManager(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MiniQTemplate miniQTemplate(MiniQConnectionManager connectionManager) {
        return new MiniQTemplate(connectionManager);
    }
}
