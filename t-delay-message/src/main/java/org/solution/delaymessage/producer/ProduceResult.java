package org.solution.delaymessage.producer;

/**
 * @author huxuewang
 */
public class ProduceResult {

    private ProduceStatus produceStatus;
    private String msgId;

    public ProduceResult() {
    }

    public ProduceResult(ProduceStatus produceStatus) {
        this.produceStatus = produceStatus;
    }


    public ProduceResult(ProduceStatus produceStatus, String msgId) {
        this.produceStatus = produceStatus;
        this.msgId = msgId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public ProduceStatus getSendStatus() {
        return produceStatus;
    }

    public void setSendStatus(ProduceStatus produceStatus) {
        this.produceStatus = produceStatus;
    }

    public static ProduceResult success(String msgId) {
        return new ProduceResult(ProduceStatus.SEND_SUCCESS, msgId);
    }

    public static ProduceResult fail(ProduceStatus produceStatus) {
        return new ProduceResult(produceStatus);
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "sendStatus=" + produceStatus +
                ", msgId='" + msgId + '\'' +
                '}';
    }
}
