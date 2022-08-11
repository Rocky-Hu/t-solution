package org.solution.delaymessage;

import org.solution.delaymessage.consumer.ConsumeStatus;
import org.solution.delaymessage.common.message.DelayMessageExt;

import java.util.concurrent.CompletableFuture;

public interface DelayMessageListener {

    CompletableFuture<ConsumeStatus> consumeMessage(DelayMessageExt message);

}
