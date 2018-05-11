package com.elasticsearch.datatransfer.schedule;

import com.elasticsearch.datatransfer.job.ArticlePuller;
import com.elasticsearch.datatransfer.job.BookPuller;
import com.elasticsearch.datatransfer.job.PatentPuller;
import com.elasticsearch.datatransfer.job.PeriodicalPuller;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
public class PullData {

    private final static Logger log = LoggerFactory.getLogger(PullData.class);


    @Configuration
    @ConditionalOnProperty(name = "job.puller.cnki-article", havingValue = "true")
    @RequiredArgsConstructor(onConstructor = @__(@Autowired))
    static class ArticleJob{

        private final ArticlePuller articlePuller;

        @Scheduled(fixedDelay = 2 * 60_000)
        public void pullO2oOrder() {
            log.info("开始执行任务 ArticlePuller ");
            articlePuller.execute();
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "job.puller.patent", havingValue = "true")
    @RequiredArgsConstructor(onConstructor = @__(@Autowired))
    static class PatentJob{

        private final PatentPuller patentPuller;

        @Scheduled(fixedDelay = 2 * 60_000)
        public void pullO2oOrder() {
            log.info("开始执行任务 PatentPuller ");
            patentPuller.execute();
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "job.puller.book", havingValue = "true")
    @RequiredArgsConstructor(onConstructor = @__(@Autowired))
    static class BookJob{

        private final BookPuller bookPuller;

        @Scheduled(fixedDelay = 2 * 60_000)
        public void pullO2oOrder() {
            log.info("开始执行任务 BookPuller ");
            bookPuller.execute();
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "job.puller.periodical", havingValue = "true")
    @RequiredArgsConstructor(onConstructor = @__(@Autowired))
    static class PeriodicalJob{

        private final PeriodicalPuller periodicalPuller;

        @Scheduled(fixedDelay = 2 * 60_000)
        public void pullO2oOrder() {
            log.info("开始执行任务 PeriodicalPuller ");
            periodicalPuller.execute();
        }
    }

}
