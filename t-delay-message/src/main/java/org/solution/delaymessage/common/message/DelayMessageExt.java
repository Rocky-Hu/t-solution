package org.solution.delaymessage.common.message;

import java.util.Arrays;

/**
 * @author huxuewang
 */
public class DelayMessageExt extends DelayMessage {

    private String id;

    private long bornTimestamp;

    private int redeliveryTimes;

    private int consumeExTimes;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public int getRedeliveryTimes() {
        return redeliveryTimes;
    }

    public void setRedeliveryTimes(int redeliveryTimes) {
        this.redeliveryTimes = redeliveryTimes;
    }

    public int getConsumeExTimes() {
        return consumeExTimes;
    }

    public void setConsumeExTimes(int consumeExTimes) {
        this.consumeExTimes = consumeExTimes;
    }

    @Override
    public String toString() {
        return "DelayMessage{" +
                "msgId='" + id + '\'' +
                "topic='" + getTopic() + '\'' +
                ", delay=" + getDelay() +
                ", timeUnit=" + getTimeUnit() +
                ", properties=" + getProperties() +
                ", body=" + Arrays.toString(getBody()) +
                ", bornTimestamp=" + bornTimestamp +
                ", redeliveryTimes=" + redeliveryTimes +
                ", consumeExTimes=" + consumeExTimes +
                '}';
    }

}
