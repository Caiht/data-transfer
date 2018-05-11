package com.elasticsearch.datatransfer.job;

import com.elasticsearch.datatransfer.models.Book;
import com.elasticsearch.datatransfer.repo.BookRepo;
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
public class BookPuller {

    private Logger logger = LoggerFactory.getLogger(BookPuller.class);

    private static final int LIMIT = 1000;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    private final StringRedisTemplate redisTemplate;

    private final BookRepo bookRepo;

    public BookPuller(NamedParameterJdbcTemplate jdbcTemplate, BookRepo bookRepo, StringRedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.bookRepo = bookRepo;
        this.redisTemplate = redisTemplate;
    }

    public void execute() {

        logger.debug("开始转移图书数据调度任务");
        Long id ;
        if (StringUtils.isNotBlank(redisTemplate.opsForValue().get("bookOffset"))) {
            id = Long.parseLong(redisTemplate.opsForValue().get("bookOffset"));
        } else {
            id = 0L;
        }

        int size = LIMIT;
        while (size == LIMIT) {
            logger.debug("单次图书转移开始");
            Map map = new HashMap();
            map.put("id", id);
            map.put("limit", LIMIT);
            val list = jdbcTemplate.query(
                    "SELECT * FROM books " +
                            "WHERE id > :id " +
                            "ORDER BY id ASC " +
                            "LIMIT :limit",
                    map, new RowMapper<Book>() {
                        @Override
                        public Book mapRow(ResultSet rs, int rowNum) throws SQLException {
                            Book book = new Book();
                            book.setId(rs.getLong("id"));
                            book.setTitle(rs.getString("title"));
                            book.setScore(rs.getDouble("score"));
                            book.setAuthor(rs.getString("author"));
                            book.setPrice(rs.getDouble("price"));
                            book.setDate(rs.getDate("date"));
                            book.setPublish(rs.getString("publish"));
                            book.setPerson(rs.getString("person"));
                            book.setTag(rs.getString("tag"));
                            book.setIntroduction(rs.getString("introduction"));
                            book.setIsbn(rs.getString("isbn"));
                            return book;
                        }
                    });

            if (!list.isEmpty()) {
                size = list.size();
                Book lastOne = Book.class.cast(list.get(list.size() - 1));

                bookRepo.saveAll(list);
                logger.info("单次图书转移结束，转移起始ID:"+id+"结束ID:"+(id+size-1));

                id = lastOne.getId();
                map.put("id", id);
            } else {
                size = 0;
            }
        }
        logger.debug("转移图书数据调度任务结束");
        redisTemplate.opsForValue().set("bookOffset", id.toString());
    }


}
