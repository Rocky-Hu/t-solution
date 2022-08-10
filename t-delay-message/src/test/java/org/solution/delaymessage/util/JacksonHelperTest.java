package org.solution.delaymessage.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.solution.delaymessage.common.message.DelayMessage;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class JacksonHelperTest {

    @Test
    public void test() throws Exception {
        final String message = "message-1";
        DelayMessage delayMessage = new DelayMessage("myTopic", 5,
                TimeUnit.SECONDS, message.getBytes(StandardCharsets.UTF_8));
        ObjectMapper objectMapper = new ObjectMapper();

        String resultStr = objectMapper.writeValueAsString(delayMessage);
        System.out.println(resultStr);

        DelayMessage delayMessage1 = objectMapper.readValue(resultStr, DelayMessage.class);
        System.out.println(new String(delayMessage1.getBody(), StandardCharsets.UTF_8));
        System.out.println("xx");
    }

}
