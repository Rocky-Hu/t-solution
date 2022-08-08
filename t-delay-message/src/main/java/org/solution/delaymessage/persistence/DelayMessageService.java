package org.solution.delaymessage.persistence;

public interface DelayMessageService {

    DelayMessageEntity get(String id);

    void insert(DelayMessageEntity entity);

    void delete(String id);

    void update(String id, Integer status);

}
