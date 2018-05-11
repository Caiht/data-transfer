package com.elasticsearch.datatransfer.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
public class DbConfig {


    @Bean(destroyMethod = "close")
    @ConfigurationProperties(prefix = "datasource.mysql")
    public HikariDataSource mysqlDataSource() {
        return new HikariDataSource();
    }


    @Bean
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
        return new NamedParameterJdbcTemplate(mysqlDataSource());
    }
}
