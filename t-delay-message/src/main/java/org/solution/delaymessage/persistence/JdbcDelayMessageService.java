package org.solution.delaymessage.persistence;

import org.solution.delaymessage.common.DelayMessageStatus;
import org.solution.delaymessage.exception.DelayMessageException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcDelayMessageService implements DelayMessageService {

    private static final String DEFAULT_SELECT_STATEMENT = "select id, topic, content, status, expire_time, create_time, modify_time from delay_message where id = ?";

    private static final String DEFAULT_INSERT_STATEMENT = "insert into delay_message(id, topic, content, status, expire_time, create_time, modify_time)" +
            "values(?,?,?,?,?,?,?)";

    private static final String DEFAULT_UPDATE_STATUS_STATEMENT = "update delay_message set status=? where id = ?";

    private static final String DEFAULT_DELETE_STATEMENT = "delete from delay_message where id = ?";

    private JdbcTemplate jdbcTemplate;

    public JdbcDelayMessageService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public DelayMessageEntity get(String id) {
        DelayMessageEntity delayMessageEntity;
        try {
            delayMessageEntity = jdbcTemplate.queryForObject(DEFAULT_SELECT_STATEMENT, new DelayMessageEntityRowMapper(), id);
        } catch (EmptyResultDataAccessException e) {
            throw new DelayMessageException("No record with requested id: " + id);
        }

        return delayMessageEntity;
    }

    public void insert(DelayMessageEntity entity) {
        try {
            jdbcTemplate.update(DEFAULT_INSERT_STATEMENT, getFields(entity));
        } catch (DuplicateKeyException e) {
            throw new DelayMessageException("Duplicate record id: " + entity.getId());
        }
    }

    public void delete(String id) {
        int count = jdbcTemplate.update(DEFAULT_DELETE_STATEMENT, id);
        if (count != 1) {
            throw new DelayMessageException("No record found with id = " + id);
        }
    }

    public void update(String id, Integer status) {
        int count = jdbcTemplate.update(DEFAULT_UPDATE_STATUS_STATEMENT, new Object[]{status, id});
        if (count != 1) {
            throw new DelayMessageException("No record found with id = " + id);
        }
    }

    private Object[] getFields(DelayMessageEntity delayMessage) {
        return new Object[]{delayMessage.getId(), delayMessage.getTopic(), delayMessage.getContent(),
                delayMessage.getStatus(), delayMessage.getExpireTime(), delayMessage.getCreateTime(), delayMessage.getModifyTime()};
    }

    private class DelayMessageEntityRowMapper implements RowMapper<DelayMessageEntity> {

        @Override
        public DelayMessageEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            DelayMessageEntity entity = new DelayMessageEntity();
            entity.setId(rs.getString("id"));
            entity.setTopic(rs.getString("topic"));
            entity.setContent(rs.getString("content"));
            entity.setStatus(DelayMessageStatus.getByCode(rs.getInt("status")));
            entity.setExpireTime(rs.getLong("expire_time"));
            entity.setCreateTime(rs.getDate("create_time"));
            entity.setModifyTime(rs.getDate("modify_time"));
            return entity;
        }

    }

}
