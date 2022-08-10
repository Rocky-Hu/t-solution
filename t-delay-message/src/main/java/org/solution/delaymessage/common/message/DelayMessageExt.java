package org.solution.delaymessage.common.message;

public class DelayMessageExt extends DelayMessage {

    private String id;

    private long bornTimestamp;

    private int redeliveryTimes;

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

    @Override
    public String toString() {
        return "DelayMessageExt{" +
                "id='" + id + '\'' +
                ", bornTimestamp=" + bornTimestamp +
                ", redeliveryTimes=" + redeliveryTimes +
                '}';
    }

}
