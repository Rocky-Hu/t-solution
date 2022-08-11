package org.solution.delaymessage;

import org.solution.delaymessage.consumer.ConsumeStatus;
import org.solution.delaymessage.common.message.DelayMessageExt;

/**
 * @author huxuewang
 */
public interface DelayMessageListener {

    ConsumeStatus consumeMessage(DelayMessageExt message);

}
