package org.solution.delaymessage.storage;

import java.util.List;

/**
 * @author huxuewang
 */
public interface DelayMessageDbStorageService {

    DelayMessageEntity get(String id);

    List<DelayMessageEntity> getByStatus(Integer status);

    void insert(DelayMessageEntity entity);

    void delete(String id);

    void delete(String[] ids);

    void deleteByStatus(Integer status);

    void updateStatus(String id, Integer status);

    void updateConsumeExTimes(String id);

}
