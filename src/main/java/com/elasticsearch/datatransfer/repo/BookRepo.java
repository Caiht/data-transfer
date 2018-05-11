package com.elasticsearch.datatransfer.repo;


import com.elasticsearch.datatransfer.models.Book;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BookRepo extends ElasticsearchRepository<Book, Long> {
}
