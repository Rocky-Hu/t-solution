package org.solution.delaymessage.consumer;

import org.solution.delaymessage.common.message.DelayMessageExt;

public interface DelayMessageConsumeService {

    ConsumeStatus consumeMessage(final DelayMessageExt msg);

}
