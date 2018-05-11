package com.elasticsearch.datatransfer.repo;

import com.elasticsearch.datatransfer.models.Periodical;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PeriodicalRepo extends ElasticsearchRepository<Periodical, Long> {
}
