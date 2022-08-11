package org.solution.delaymessage.common.message;

import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author huxuewang
 */
public class DelayMessage implements Serializable {

    private String topic;
    private long delay;
    private TimeUnit timeUnit;
    private Map<String, String> properties;

    /**
     * String UTF-8 encoded byte array
     */
    private byte[] body;

    public DelayMessage() {
    }

    public DelayMessage(String topic, long delay, TimeUnit timeUnit, byte[] body) {
        this(topic, null, null, delay, timeUnit, body);
    }

    public DelayMessage(String topic, String keys, String tags, long delay, TimeUnit timeUnit, byte[] body) {
        Assert.hasLength(topic, "topic can't be empty");
        Assert.notNull(delay, "delay can't be null");
        Assert.notNull(timeUnit, "timeUnit can't be null");
        Assert.notNull(body, "body can't be null");

        if (timeUnit == TimeUnit.NANOSECONDS || timeUnit == TimeUnit.MICROSECONDS || timeUnit == TimeUnit.MILLISECONDS) {
            throw new IllegalArgumentException("Unsupported time unit!");
        }

        if (keys != null && keys.length() > 0) {
            this.setKeys(keys);
        }

        if (tags != null && tags.length() > 0) {
            this.setTags(tags);
        }

        this.topic = topic;
        this.delay = delay;
        this.timeUnit = timeUnit;
        this.body = body;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getTags() {
        return this.getProperty(DelayMessageConstant.PROPERTY_TAGS);
    }

    public void setTags(String tags) {
        this.setProperty(DelayMessageConstant.PROPERTY_TAGS, tags);
    }

    public void setKeys(String keys) {
        this.setProperty(DelayMessageConstant.PROPERTY_KEYS, keys);
    }

    public String getKeys() {
        return this.getProperty(DelayMessageConstant.PROPERTY_KEYS);
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<String, String>();
        }

        return this.properties.get(name);
    }

    public void setProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        this.properties.put(name, value);
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "DelayMessage{" +
                "topic='" + topic + '\'' +
                ", delay=" + delay +
                ", timeUnit=" + timeUnit +
                ", properties=" + properties +
                ", body=" + Arrays.toString(body) +
                '}';
    }

}
