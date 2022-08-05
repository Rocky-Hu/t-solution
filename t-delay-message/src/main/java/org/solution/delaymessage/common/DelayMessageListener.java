package org.solution.delaymessage.common;

import java.util.function.Consumer;

public interface DelayMessageListener<T> extends Consumer<T> {
}
