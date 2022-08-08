package org.solution.delaymessage.persistence;

import org.springframework.data.repository.CrudRepository;

public interface DelayMessageRepository extends CrudRepository<DelayMessageEntity, String> {
}
