package org.solution.delaymessage.storage;

import org.solution.delaymessage.common.message.DelayMessageConstant;
import org.solution.delaymessage.common.message.DelayMessageExt;
import org.solution.delaymessage.common.message.DelayMessageStatus;
import org.solution.delaymessage.utils.JacksonHelper;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author huxuewang
 */
public class DelayMessageEntity implements Serializable {

    private String id;
    private String topic;
    private String content;
    private DelayMessageStatus status;
    private Long expireTime;
    private Long delay;
    private Integer timeUnit;
    private String tags;
    private String keys;
    private String properties;
    private Long bornTime;
    private Integer redeliveryTimes;

    private Integer consumeExTimes;
    private Date createTime;
    private Date modifyTime;

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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public DelayMessageStatus getStatus() {
        return status;
    }

    public void setStatus(DelayMessageStatus status) {
        this.status = status;
    }

    public Long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Long expireTime) {
        this.expireTime = expireTime;
    }

    public Long getDelay() {
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    public Integer getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(Integer timeUnit) {
        this.timeUnit = timeUnit;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Long getBornTime() {
        return bornTime;
    }

    public void setBornTime(Long bornTime) {
        this.bornTime = bornTime;
    }

    public Integer getRedeliveryTimes() {
        return redeliveryTimes;
    }

    public void setRedeliveryTimes(Integer redeliveryTimes) {
        this.redeliveryTimes = redeliveryTimes;
    }

    public Integer getConsumeExTimes() {
        return consumeExTimes;
    }

    public void setConsumeExTimes(Integer consumeExTimes) {
        this.consumeExTimes = consumeExTimes;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public void init(DelayMessageExt delayMessageExt) {
        this.setId(delayMessageExt.getId());
        this.setTopic(delayMessageExt.getTopic());
        this.setContent(new String(delayMessageExt.getBody(), StandardCharsets.UTF_8));
        this.setStatus(DelayMessageStatus.CONSUME_PENDING);
        this.setExpireTime(calculateExpireTime(delayMessageExt.getBornTimestamp(), delayMessageExt.getDelay(), delayMessageExt.getTimeUnit()));
        this.setDelay(delayMessageExt.getDelay());
        this.setTimeUnit(fromTimeUnit(delayMessageExt.getTimeUnit()));
        this.setTags(delayMessageExt.getProperty(DelayMessageConstant.PROPERTY_TAGS));
        this.setKeys(delayMessageExt.getProperty(DelayMessageConstant.PROPERTY_KEYS));
        this.setProperties(JacksonHelper.writeValueAsString(delayMessageExt.getProperties()));
        this.setBornTime(delayMessageExt.getBornTimestamp());
        this.setRedeliveryTimes(delayMessageExt.getRedeliveryTimes());
        this.setConsumeExTimes(delayMessageExt.getConsumeExTimes());
    }

    public DelayMessageExt to() {
        DelayMessageExt message = new DelayMessageExt();
        message.setId(this.getId());
        message.setTopic(this.getTopic());
        message.setBody(this.getContent().getBytes(StandardCharsets.UTF_8));
        message.setDelay(this.getDelay());
        message.setTimeUnit(toTimeUnit(this.getTimeUnit()));
        message.setProperties(JacksonHelper.readValue(this.getProperties(), Map.class));
        message.setBornTimestamp(this.getBornTime());
        message.setRedeliveryTimes(this.getRedeliveryTimes());
        message.setConsumeExTimes(this.getConsumeExTimes());
        return message;
    }

    private Long calculateExpireTime(long bornTimestamp, long delay, TimeUnit timeUnit) {
        if (TimeUnit.SECONDS == timeUnit) {
            return bornTimestamp + delay;
        } else if (TimeUnit.MINUTES == timeUnit) {
            return bornTimestamp + 60 * delay;
        } else if (TimeUnit.HOURS == timeUnit) {
            return bornTimestamp + 60 * 60 * delay;
        } else if (TimeUnit.DAYS == timeUnit) {
            return bornTimestamp + 24 * 60 * 60 * delay;
        } else {
            throw new IllegalArgumentException("Unsupported time unit!");
        }
    }

    private Integer fromTimeUnit(TimeUnit timeUnit) {
        if (timeUnit == TimeUnit.MILLISECONDS) {
            return 0;
        } else if (timeUnit == TimeUnit.SECONDS) {
            return 1;
        } else if (timeUnit == TimeUnit.MINUTES) {
            return 2;
        } else if (timeUnit == TimeUnit.HOURS) {
            return 3;
        } else if (timeUnit == TimeUnit.DAYS) {
            return 4;
        }

        throw new IllegalArgumentException("Unsupported time unit!");
    }

    private TimeUnit toTimeUnit(Integer code) {
        if (code == 0) {
            return TimeUnit.MILLISECONDS;
        } else if (code == 1) {
            return TimeUnit.SECONDS;
        } else if (code == 2) {
            return TimeUnit.MINUTES;
        } else if (code == 3) {
            return TimeUnit.HOURS;
        } else if (code == 4) {
            return TimeUnit.DAYS;
        }

        throw new IllegalArgumentException("Unsupported time unit code!");
    }

}
