package org.solution.delaymessage.persistence;

import org.solution.delaymessage.common.DelayMessage;
import org.solution.delaymessage.common.DelayMessageStatus;
import org.solution.delaymessage.util.JacksonHelper;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DelayMessageEntity implements Serializable {

    private String id;
    private String topic;
    private String content;
    private DelayMessageStatus status;
    private Long expireTime;
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

    public DelayMessageEntity from(DelayMessage delayMessage) {
        DelayMessageEntity entity = new DelayMessageEntity();
        entity.setId(delayMessage.getId());
        entity.setTopic(delayMessage.getTopic());
        entity.setStatus(DelayMessageStatus.CONSUME_PENDING);
        entity.setContent(JacksonHelper.writeValueAsString(delayMessage));
        entity.setExpireTime(calculateExpireTime(delayMessage.getBornTimestamp(), delayMessage.getDelay(), delayMessage.getTimeUnit()));
        return entity;
    }

    private Long calculateExpireTime(long bornTimestamp, long delay, TimeUnit timeUnit) {
        if (TimeUnit.MILLISECONDS == timeUnit) {
            return bornTimestamp + delay;
        } else if (TimeUnit.SECONDS == timeUnit) {
            return bornTimestamp + 1000 * delay;
        } else if (TimeUnit.MINUTES == timeUnit) {
            return bornTimestamp + 60 * 1000 * delay;
        } else if (TimeUnit.HOURS == timeUnit) {
            return bornTimestamp + 60 * 60 * 1000 * delay;
        } else if (TimeUnit.DAYS == timeUnit) {
            return bornTimestamp + 24 * 60 * 60 * 1000 * delay;
        } else {
            throw new IllegalArgumentException("Unsupported time unit!");
        }
    }

}
