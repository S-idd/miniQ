package dev.miniq.spring;

import dev.miniq.client.MiniQException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MiniQAutoConfigurationTest {
    private static final String IMPORTS =
            "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MiniQAutoConfiguration.class));

    @Test
    void importsFileListsTheAutoConfiguration() throws Exception {
        List<String> lines = Files.readAllLines(Path.of("src/main/resources", IMPORTS), StandardCharsets.UTF_8);
        assertThat(lines).containsExactly(MiniQAutoConfiguration.class.getName());
        assertThat(ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader()).getCandidates())
                .contains(MiniQAutoConfiguration.class.getName());
    }

    @Test
    void realApplicationDiscoversBeansWithoutNamingTheAutoConfiguration() throws Exception {
        int deadPort = BrokerFixture.unusedPort();
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(EmptyConfig.class)
                .web(WebApplicationType.NONE)
                .properties("spring.main.banner-mode=off", "miniq.port=" + deadPort)
                .run()) {
            assertThat(context.getBeansOfType(MiniQConnectionManager.class)).hasSize(1);
            assertThat(context.getBeansOfType(MiniQTemplate.class)).hasSize(1);
            assertThat(context.getBean(MiniQProperties.class).getPort()).isEqualTo(deadPort);
        }
    }

    @Test
    void contextStartsAndBeansExistWhenNoBrokerIsRunning() throws Exception {
        int deadPort = BrokerFixture.unusedPort();
        runner.withPropertyValues("miniq.host=127.0.0.1", "miniq.port=" + deadPort).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(MiniQConnectionManager.class);
            assertThat(context).hasSingleBean(MiniQTemplate.class);
            assertThatThrownBy(() -> context.getBean(MiniQTemplate.class).send("orders", "x"))
                    .isInstanceOf(MiniQException.class)
                    .hasMessageContaining("127.0.0.1:" + deadPort)
                    .satisfies(failure -> assertThat(((MiniQException) failure).errorCode()).isEmpty());
        });
    }

    @Test
    void disabledPropertyRegistersNoMiniQBeans() {
        runner.withPropertyValues("miniq.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(MiniQConnectionManager.class);
            assertThat(context).doesNotHaveBean(MiniQTemplate.class);
            assertThat(context).doesNotHaveBean(MiniQProperties.class);
            List<String> miniqBeans = new ArrayList<>();
            for (String name : context.getBeanDefinitionNames()) {
                Class<?> type = context.getType(name);
                if (type != null && type.getPackageName().equals(MiniQAutoConfiguration.class.getPackageName())) {
                    miniqBeans.add(name);
                }
            }
            assertThat(miniqBeans).isEmpty();
        });
    }

    @Test
    void enabledByDefaultAndWhenTrue() {
        runner.run(context -> assertThat(context).hasSingleBean(MiniQTemplate.class));
        runner.withPropertyValues("miniq.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(MiniQTemplate.class));
    }

    @Test
    void userDefinedConnectionManagerWins() {
        runner.withUserConfiguration(UserManagerConfig.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(MiniQConnectionManager.class))
                    .containsOnlyKeys("userManager");
            assertThat(context).hasSingleBean(MiniQTemplate.class);
        });
    }

    @Test
    void userDefinedTemplateWins() {
        runner.withUserConfiguration(UserTemplateConfig.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(MiniQTemplate.class)).containsOnlyKeys("userTemplate");
            assertThat(context).hasSingleBean(MiniQConnectionManager.class);
        });
    }

    @Test
    void defaultsBind() {
        runner.run(context -> {
            MiniQProperties properties = context.getBean(MiniQProperties.class);
            assertThat(properties.isEnabled()).isTrue();
            assertThat(properties.getHost()).isEqualTo("localhost");
            assertThat(properties.getPort()).isEqualTo(7070);
            assertThat(properties.getConnectTimeoutMs()).isEqualTo(2_000);
            assertThat(properties.getReplyTimeoutMs()).isEqualTo(5_000);
        });
    }

    @Test
    void propertiesBind() {
        runner.withPropertyValues("miniq.host=broker.example", "miniq.port=1234",
                "miniq.connect-timeout-ms=1500", "miniq.reply-timeout-ms=2500").run(context -> {
            MiniQProperties properties = context.getBean(MiniQProperties.class);
            assertThat(properties.getHost()).isEqualTo("broker.example");
            assertThat(properties.getPort()).isEqualTo(1234);
            assertThat(properties.getConnectTimeoutMs()).isEqualTo(1_500);
            assertThat(properties.getReplyTimeoutMs()).isEqualTo(2_500);
        });
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "miniq.host=|miniq.host must not be blank",
            "miniq.host=   |miniq.host must not be blank",
            "miniq.port=0|miniq.port must be between 1 and 65535",
            "miniq.port=65536|miniq.port must be between 1 and 65535",
            "miniq.connect-timeout-ms=0|miniq.connect-timeout-ms must be at least 1",
            "miniq.reply-timeout-ms=0|miniq.reply-timeout-ms must be at least 1"
    }, ignoreLeadingAndTrailingWhitespace = false)
    void invalidValueFailsStartupNamingTheProperty(String property, String expectedMessage) {
        runner.withPropertyValues(property).run(context -> {
            assertThat(context).hasFailed();
            String messages = causalMessages(context.getStartupFailure());
            assertThat(messages).contains(expectedMessage).contains("prefix=miniq");
        });
    }

    private static String causalMessages(Throwable failure) {
        StringBuilder all = new StringBuilder();
        for (Throwable t = failure; t != null; t = t.getCause()) all.append(t.getMessage()).append('\n');
        return all.toString();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class EmptyConfig { }

    @Configuration(proxyBeanMethods = false)
    static class UserManagerConfig {
        @Bean
        MiniQConnectionManager userManager() {
            return new MiniQConnectionManager(new MiniQProperties());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class UserTemplateConfig {
        @Bean
        MiniQTemplate userTemplate(MiniQConnectionManager manager) {
            return new MiniQTemplate(manager);
        }
    }
}
