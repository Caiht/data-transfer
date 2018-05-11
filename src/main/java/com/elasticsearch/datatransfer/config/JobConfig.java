package com.elasticsearch.datatransfer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "job.puller")
@Data
public class JobConfig {

    private Boolean cnkiArticle = false;

    private Boolean patent = false;

    private Boolean book = false;

    private Boolean periodical = false;
}
