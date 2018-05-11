package com.elasticsearch.datatransfer.job;


import com.elasticsearch.datatransfer.models.Periodical;
import com.elasticsearch.datatransfer.repo.PeriodicalRepo;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


@Component
public class PeriodicalPuller {

    private Logger logger = LoggerFactory.getLogger(PeriodicalPuller.class);

    private static final int LIMIT = 1000;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    private final StringRedisTemplate redisTemplate;

    private final PeriodicalRepo periodicalRepo;

    public PeriodicalPuller(NamedParameterJdbcTemplate jdbcTemplate, PeriodicalRepo periodicalRepo, StringRedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.periodicalRepo = periodicalRepo;
        this.redisTemplate = redisTemplate;
    }

    public void execute() {

        Long id ;
        if (StringUtils.isNotBlank(redisTemplate.opsForValue().get("periodicalOffset"))) {
            id = Long.parseLong(redisTemplate.opsForValue().get("periodicalOffset"));
        } else {
            id = 0L;
        }
        logger.debug("开始转移期刊数据调度任务");
        Map map = new HashMap();
        map.put("id", id);
        map.put("limit", LIMIT);


        int size = LIMIT;
        while (size == LIMIT) {
            logger.debug("单次期刊转移开始");

            val list = jdbcTemplate.query(
                    "SELECT * FROM periodicals " +
                            "WHERE id > :id " +
                            "ORDER BY id ASC " +
                            "LIMIT :limit",
                    map, new RowMapper<Periodical>() {
                        @Override
                        public Periodical mapRow(ResultSet rs, int rowNum) throws SQLException {
                            Periodical periodical = new Periodical();
                            periodical.setId(rs.getLong("id"));
                            periodical.setTitle(rs.getString("title"));
                            periodical.setAuthor(rs.getString("author"));
                            periodical.setDate(rs.getDate("date"));
                            periodical.setPublisher(rs.getString("publisher"));
                            periodical.setType(rs.getString("type"));
                            periodical.setIntroduction(rs.getString("introduction"));

                            return periodical;
                        }
                    });

            if (!list.isEmpty()) {
                size = list.size();
                Periodical lastOne = Periodical.class.cast(list.get(list.size() - 1));

                periodicalRepo.saveAll(list);
                logger.info("单次期刊转移结束，转移起始ID:" + id + "结束ID:" + (id + size - 1));

                id = lastOne.getId();
                map.put("id", id);
            } else {
                size = 0;
            }
        }
        logger.debug("转移期刊数据调度任务结束");
        redisTemplate.opsForValue().set("periodicalOffset", id.toString());
    }


}
