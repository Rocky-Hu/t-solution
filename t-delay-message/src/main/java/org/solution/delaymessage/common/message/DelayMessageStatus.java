package org.solution.delaymessage.common.message;

public enum DelayMessageStatus {

    CONSUME_PENDING(0, "待消费"),
    CONSUME_SUCCESS(1, "消费成功"),
    CONSUME_EXCEPTION(2, "消费异常");

    private int code;
    private String desc;

    DelayMessageStatus(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DelayMessageStatus getByCode(int code) {
        for (DelayMessageStatus status : DelayMessageStatus.values()) {
            if (code == status.code) {
                return status;
            }
        }

        return null;
    }

}
