package com.elasticsearch.datatransfer.repo;

import com.elasticsearch.datatransfer.models.Patent;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PatentRepo extends ElasticsearchRepository<Patent, Long> {
}
