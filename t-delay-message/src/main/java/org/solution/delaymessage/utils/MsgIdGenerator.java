package org.solution.delaymessage.utils;

import java.util.UUID;

public class MsgIdGenerator {

    public static String generate() {
        return UUID.randomUUID().toString();
    }

}
