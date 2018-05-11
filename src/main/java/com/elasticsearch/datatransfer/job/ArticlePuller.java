package com.elasticsearch.datatransfer.job;


import com.elasticsearch.datatransfer.models.CnkiArticle;
import com.elasticsearch.datatransfer.repo.CnkiArticleRepo;
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
public class ArticlePuller {

    private Logger logger = LoggerFactory.getLogger(ArticlePuller.class);

    private static final int LIMIT = 1000;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    private final StringRedisTemplate redisTemplate;

    private final CnkiArticleRepo cnkiArticleRepo;

    public ArticlePuller(NamedParameterJdbcTemplate jdbcTemplate, CnkiArticleRepo cnkiArticleRepo, StringRedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.cnkiArticleRepo = cnkiArticleRepo;
        this.redisTemplate = redisTemplate;
    }

    public void execute() {

        logger.debug("开始转移硕士论文数据调度任务");
        Long id ;
        if (StringUtils.isNotBlank(redisTemplate.opsForValue().get("articleOffset"))) {
            id = Long.parseLong(redisTemplate.opsForValue().get("articleOffset"));
        } else {
            id = 0L;
        }
        Map map =new HashMap();
        map.put("id",id);
        map.put("limit",LIMIT);


        int size = LIMIT;
        while (size == LIMIT) {
            logger.debug("单次硕士论文转移开始");

            val list = jdbcTemplate.query(
                    "SELECT * FROM cnki_articles " +
                            "WHERE id > :id " +
                            "ORDER BY id ASC " +
                            "LIMIT :limit",
                    map, new RowMapper<CnkiArticle>() {
                        @Override
                        public CnkiArticle mapRow(ResultSet rs, int rowNum) throws SQLException {
                            CnkiArticle article = new CnkiArticle();
                            article.setId(rs.getLong("id"));
                            article.setTitle(rs.getString("title"));
                            article.setAuthor(rs.getString("author"));
                            article.setTeacher(rs.getString("teacher"));
                            article.setUniversity(rs.getString("university"));
                            article.setDate(rs.getDate("date"));
                            article.setType(rs.getString("type"));
                            article.setIntroduction(rs.getString("introduction"));
                            return article;
                        }
                    });

            if (!list.isEmpty()) {
                size = list.size();
                CnkiArticle lastOne = CnkiArticle.class.cast(list.get(list.size() - 1));

                cnkiArticleRepo.saveAll(list);
                logger.info("单次硕士论文转移结束，转移起始ID:"+id+"结束ID:"+(id+size-1));

                id = lastOne.getId();
                map.put("id",id);
            } else {
                size = 0;
            }
        }
        logger.debug("转移硕士论文数据调度任务结束");
        redisTemplate.opsForValue().set("articleOffset", id.toString());
    }


}
