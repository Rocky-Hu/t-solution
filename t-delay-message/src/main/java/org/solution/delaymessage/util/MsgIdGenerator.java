package org.solution.delaymessage.util;

import java.util.UUID;

public class MsgIdGenerator {

    public static String generate() {
        return UUID.randomUUID().toString();
    }

}
