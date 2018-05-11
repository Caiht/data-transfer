package com.elasticsearch.datatransfer.job;


import com.elasticsearch.datatransfer.models.Patent;
import com.elasticsearch.datatransfer.repo.PatentRepo;
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
public class PatentPuller {
    private Logger logger = LoggerFactory.getLogger(PatentPuller.class);

    private static final int LIMIT = 1000;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    private final StringRedisTemplate redisTemplate;

    private final PatentRepo patentRepo;

    public PatentPuller(NamedParameterJdbcTemplate jdbcTemplate, PatentRepo patentRepo, StringRedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.patentRepo = patentRepo;
        this.redisTemplate = redisTemplate;
    }

    public void execute() {


        logger.debug("开始转移专利数据调度任务");
        Long id ;
        if (StringUtils.isNotBlank(redisTemplate.opsForValue().get("patentOffset"))) {
            id = Long.parseLong(redisTemplate.opsForValue().get("patentOffset"));
        } else {
            id = 0L;
        }
        Map map =new HashMap();
        map.put("id",id);
        map.put("limit",LIMIT);
        int size = LIMIT;
        while (size == LIMIT) {
            logger.debug("单次专利转移开始");

            val list = jdbcTemplate.query(
                    "SELECT * FROM patents " +
                            "WHERE id > :id " +
                            "ORDER BY id ASC " +
                            "LIMIT :limit",
                    map, new RowMapper<Patent>() {
                        @Override
                        public Patent mapRow(ResultSet rs, int rowNum) throws SQLException {
                            Patent patent = new Patent();
                            patent.setId(rs.getLong("id"));
                            patent.setTitle(rs.getString("title"));
                            patent.setRequestNumber(rs.getString("requestNumber"));
                            patent.setRequestDate(rs.getDate("requestDate"));
                            patent.setPublicationNumber(rs.getString("publicationNumber"));
                            patent.setPublicationDate(rs.getDate("publicationDate"));
                            patent.setProposer(rs.getString("proposer"));
                            patent.setInventor(rs.getString("inventor"));
                            patent.setIntroduction(rs.getString("introduction"));
                            patent.setType(rs.getString("type"));
                            return patent;
                        }
                    });

            if (!list.isEmpty()) {
                size = list.size();
                Patent lastOne = Patent.class.cast(list.get(list.size() - 1));

                patentRepo.saveAll(list);
                logger.info("单次专利转移结束，转移起始ID:"+id+"结束ID:"+(id+size-1));

                id = lastOne.getId();
                map.put("id", id);
            } else {
                size = 0;
            }
        }
        logger.debug("转移专利数据调度任务结束");
        redisTemplate.opsForValue().set("patentOffset", id.toString());
    }

}
