package org.solution.delaymessage.common;

import org.solution.delaymessage.util.CommonUtils;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DelayMessage implements Serializable {

    private String id;
    private String topic;
    private long delay;
    private TimeUnit timeUnit;
    private long bornTimestamp;
    private Map<String, String> properties;

    /**
     * String UTF-8 encoded byte array
     */
    private byte[] body;

    public DelayMessage() {
    }

    public DelayMessage(String topic, long delay, TimeUnit timeUnit, byte[] body) {
        Assert.hasLength(topic, "topic can't be empty");
        Assert.notNull(delay, "delay can't be null");
        Assert.notNull(timeUnit, "timeUnit can't be null");
        Assert.notNull(body, "body can't be null");
        this.id = CommonUtils.generateUUID();
        this.topic = topic;
        this.delay = delay;
        this.timeUnit = timeUnit;
        this.bornTimestamp = System.currentTimeMillis();
        this.body = body;
    }

    public DelayMessage(String id, String topic, long delay, TimeUnit timeUnit, byte[] body) {
        Assert.hasLength(id, "id can't be empty");
        Assert.hasLength(topic, "topic can't be empty");
        Assert.notNull(delay, "delay can't be null");
        Assert.notNull(timeUnit, "timeUnit can't be null");
        Assert.notNull(body, "body can't be null");
        this.id = id;
        this.topic = topic;
        this.delay = delay;
        this.timeUnit = timeUnit;
        this.bornTimestamp = System.currentTimeMillis();
        this.body = body;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
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
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", delay=" + delay +
                ", timeUnit=" + timeUnit +
                ", bornTimestamp=" + bornTimestamp +
                ", properties=" + properties +
                ", body=" + Arrays.toString(body) +
                '}';
    }

}
