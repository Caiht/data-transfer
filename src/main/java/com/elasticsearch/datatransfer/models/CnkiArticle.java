package com.elasticsearch.datatransfer.models;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.Date;

@Data
@Document(indexName = "cnki_article", type = "cnki_article", useServerConfiguration = true, createIndex = false)
public class CnkiArticle {

    @Id
    private Long id;

    private String title;

    private String author;

    private String teacher;

    private String university;

    private Date date;

    private String type;

    private String introduction;
}
