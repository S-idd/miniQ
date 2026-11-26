package dev.miniq.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

/**
 * Connection settings bound from the {@code miniq.*} properties.
 *
 * <p>
 * The class validates itself: Spring Boot applies a {@link Validator}
 * implemented by a
 * {@code @ConfigurationProperties} target during binding, so an invalid value
 * fails context startup
 * with a binding error that names the offending {@code miniq.*} field. This
 * avoids requiring a Jakarta
 * Bean Validation provider on the application classpath.
 */
@ConfigurationProperties("miniq")
public class MiniQProperties implements Validator {
    /** Whether the MiniQ auto-configuration is active. */
    private boolean enabled = true;
    /** Broker host name or address. */
    private String host = "localhost";
    /** Broker TCP port. */
    private int port = 7070;
    /** TCP connect timeout in milliseconds. */
    private int connectTimeoutMs = 2_000;
    /** How long a request waits for the broker's reply, in milliseconds. */
    private long replyTimeoutMs = 5_000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public long getReplyTimeoutMs() {
        return replyTimeoutMs;
    }

    public void setReplyTimeoutMs(long replyTimeoutMs) {
        this.replyTimeoutMs = replyTimeoutMs;
    }

    @Override
    public boolean supports(Class<?> type) {
        return MiniQProperties.class.isAssignableFrom(type);
    }

    @Override
    public void validate(Object target, Errors errors) {
        MiniQProperties properties = (MiniQProperties) target;
        if (properties.host == null || properties.host.isBlank()) {
            errors.rejectValue("host", "miniq.host.blank", "miniq.host must not be blank");
        }
        if (properties.port < 1 || properties.port > 65_535) {
            errors.rejectValue("port", "miniq.port.range", "miniq.port must be between 1 and 65535");
        }
        if (properties.connectTimeoutMs < 1) {
            errors.rejectValue("connectTimeoutMs", "miniq.connect-timeout-ms.min",
                    "miniq.connect-timeout-ms must be at least 1");
        }
        if (properties.replyTimeoutMs < 1) {
            errors.rejectValue("replyTimeoutMs", "miniq.reply-timeout-ms.min",
                    "miniq.reply-timeout-ms must be at least 1");
        }
    }
}
