package com.elasticsearch.datatransfer.repo;


import com.elasticsearch.datatransfer.models.CnkiArticle;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CnkiArticleRepo extends ElasticsearchRepository<CnkiArticle, Long> {
}
