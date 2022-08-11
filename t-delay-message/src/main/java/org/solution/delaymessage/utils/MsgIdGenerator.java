package org.solution.delaymessage.utils;

import java.util.UUID;

/**
 * @author huxuewang
 */
public class MsgIdGenerator {

    public static String generate() {
        return UUID.randomUUID().toString();
    }

}
