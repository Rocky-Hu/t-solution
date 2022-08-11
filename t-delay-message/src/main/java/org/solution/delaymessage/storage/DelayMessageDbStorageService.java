package org.solution.delaymessage.storage;

import java.util.List;

public interface DelayMessageDbStorageService {

    DelayMessageEntity get(String id);

    List<DelayMessageEntity> getByStatus(Integer status);

    void insert(DelayMessageEntity entity);

    void delete(String id);

    void delete(String[] ids);

    void deleteByStatus(Integer status);

    void update(String id, Integer status);

}
