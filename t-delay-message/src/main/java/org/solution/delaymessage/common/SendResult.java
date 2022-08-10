package org.solution.delaymessage.common;

public class SendResult {

    private SendStatus sendStatus;

    private String msgId;

    public SendResult() {
    }

    public SendResult(SendStatus sendStatus, String msgId) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }

    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "sendStatus=" + sendStatus +
                ", msgId='" + msgId + '\'' +
                '}';
    }
}
