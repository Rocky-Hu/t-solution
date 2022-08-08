package org.solution.delaymessage.persistence;

import org.solution.delaymessage.common.DelayMessageStatus;

import java.io.Serializable;
import java.util.Date;

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

}
