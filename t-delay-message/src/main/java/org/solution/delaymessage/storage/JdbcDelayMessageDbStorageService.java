package org.solution.delaymessage.storage;

import org.solution.delaymessage.common.message.DelayMessageStatus;
import org.solution.delaymessage.exception.DelayMessageException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author huxuewang
 */
public class JdbcDelayMessageDbStorageService implements DelayMessageDbStorageService {

    private static final String DEFAULT_FIELDS = "id, topic, content, status, expire_time, delay, time_unit, tags, " +
            "keys, properties, born_time, redelivery_times, consume_ex_times" +
            "create_time, modify_time";

    private static final String DEFAULT_SELECT_STATEMENT = "select " + DEFAULT_FIELDS + " from delay_message where id = ?";

    private static final String DEFAULT_SELECT_BY_STATUS_STATEMENT = "select " + DEFAULT_FIELDS + " from delay_message where status = ?";
    private static final String DEFAULT_INSERT_STATEMENT = "insert into delay_message(id, topic, content, status, expire_time, delay, time_unit, tags, keys, " +
            "properties, born_time, redelivery_times, consume_ex_times)" +
            "values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String DEFAULT_UPDATE_STATUS_STATEMENT = "update delay_message set status = ? where id = ?";

    private static final String DEFAULT_UPDATE_CONSUME_EX_TIMES_STATEMENTS = "update delay_message set consume_ex_times = consume_ex_times + 1 where id = ?";

    private static final String DEFAULT_DELETE_STATEMENT = "delete from delay_message where id = ?";

    private static final String DEFAULT_DELETE_BATCH_STATEMENT = "delete from delay_message where id in (?)";

    private static final String DEFAULT_DELETE_BY_STATUS_STATEMENT = "delete from delay_message where status = ?";

    private JdbcTemplate jdbcTemplate;

    private RowMapper<DelayMessageEntity> rowMapper = new DelayMessageEntityRowMapper();

    public JdbcDelayMessageDbStorageService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public DelayMessageEntity get(String id) {
        DelayMessageEntity delayMessageEntity;
        try {
            delayMessageEntity = jdbcTemplate.queryForObject(DEFAULT_SELECT_STATEMENT, rowMapper, id);
        } catch (EmptyResultDataAccessException e) {
            throw new DelayMessageException("No record with requested id: " + id);
        }

        return delayMessageEntity;
    }

    @Override
    public List<DelayMessageEntity> getByStatus(Integer status) {
        return jdbcTemplate.query(DEFAULT_SELECT_BY_STATUS_STATEMENT, rowMapper, status);
    }

    public void insert(DelayMessageEntity entity) {
        try {
            jdbcTemplate.update(DEFAULT_INSERT_STATEMENT, getFields(entity));
        } catch (DuplicateKeyException e) {
            throw new DelayMessageException("Duplicate record id: " + entity.getId());
        }
    }

    public void delete(String id) {
        jdbcTemplate.update(DEFAULT_DELETE_STATEMENT, id);
    }

    @Override
    public void delete(String[] ids) {
        jdbcTemplate.update(DEFAULT_DELETE_BATCH_STATEMENT, Arrays.asList(ids));
    }

    @Override
    public void deleteByStatus(Integer status) {
        jdbcTemplate.update(DEFAULT_DELETE_BY_STATUS_STATEMENT, status);
    }

    public void updateStatus(String id, Integer status) {
        int count = jdbcTemplate.update(DEFAULT_UPDATE_STATUS_STATEMENT, new Object[]{status, id});
        if (count != 1) {
            throw new DelayMessageException("No record found with id = " + id);
        }
    }

    public void updateConsumeExTimes(String id) {
        int count = jdbcTemplate.update(DEFAULT_UPDATE_CONSUME_EX_TIMES_STATEMENTS, new Object[]{id});
        if (count != 1) {
            throw new DelayMessageException("No record found with id = " + id);
        }
    }

    private Object[] getFields(DelayMessageEntity entity) {
        return new Object[]{entity.getId(), entity.getTopic(), entity.getContent(),
                entity.getStatus().getCode(), entity.getExpireTime(), entity.getDelay(), entity.getTimeUnit(), entity.getTags(), entity.getKeys(),
                entity.getProperties(), entity.getBornTime(), entity.getRedeliveryTimes(), entity.getConsumeExTimes()};
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
            entity.setDelay(rs.getLong("delay"));
            entity.setTimeUnit(rs.getInt("time_unit"));
            entity.setTags(rs.getString("tags"));
            entity.setKeys(rs.getString("keys"));
            entity.setProperties(rs.getString("properties"));
            entity.setRedeliveryTimes(rs.getInt("redelivery_times"));
            entity.setConsumeExTimes(rs.getInt("consume_ex_times"));
            entity.setCreateTime(rs.getDate("create_time"));
            entity.setModifyTime(rs.getDate("modify_time"));
            return entity;
        }

    }

}
