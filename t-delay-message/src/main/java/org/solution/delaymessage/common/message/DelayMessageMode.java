package org.solution.delaymessage.common.message;

public enum DelayMessageMode {

    BT(0, "补推模式"),
    IGNORE(1, "忽略模式");

    private int code;
    private String desc;

    DelayMessageMode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static DelayMessageMode getByCode(int code) {
        for (DelayMessageMode status : DelayMessageMode.values()) {
            if (code == status.code) {
                return status;
            }
        }

        return null;
    }

}
