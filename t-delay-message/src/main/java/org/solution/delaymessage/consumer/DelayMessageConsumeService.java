package org.solution.delaymessage.consumer;

import org.solution.delaymessage.common.message.DelayMessageExt;

/**
 * @author huxuewang
 */
public interface DelayMessageConsumeService {

    void consumeMessage(final DelayMessageExt msg);

}
