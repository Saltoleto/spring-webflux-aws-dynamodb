package com.boottech.webfluxdynamodb.web;


import com.boottech.webfluxdynamodb.domain.Author;
import com.boottech.webfluxdynamodb.repository.AuthorRepository;
import com.boottech.webfluxdynamodb.repository.TesteImpl;
import com.boottech.webfluxdynamodb.web.dto.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;

@RestController
@RequestMapping("/author")
public class AuthorController {

    private final AuthorRepository repository;
    private final TesteImpl teste;

    @Autowired
    public AuthorController(AuthorRepository repository, TesteImpl teste) {
        this.repository = repository;
        this.teste = teste;
    }

    @GetMapping()
    public Mono<PageImpl<Author>> getAllAuthors(@RequestParam int page, @RequestParam int pageSize) {
        return teste.findAllWithPagination(PageRequest.of(page,pageSize));
    }

    @GetMapping("/count")
    public Mono<ApiResponse> authorCount() {
        return repository.count()
                .map(count -> new ApiResponse(count, MessageFormat.format("Count authors: {0}", count)));
    }

    @GetMapping("/{id}")
    public Mono<ApiResponse> getByAuthorId(@PathVariable String id) {
        return repository.findById(id)
                .map(book -> new ApiResponse(book, MessageFormat.format("Result found", book)))
                .defaultIfEmpty(new ApiResponse(null, "Author not found"));
    }

    @PostMapping()
    public Mono<ApiResponse> create(@RequestBody Mono<Author> author) {
        return author
                .flatMap(repository::save)
                .map(author1 ->  new ApiResponse(author1, "Author successfully created"));
    }
    @PutMapping("/{id}")
    public Mono<ApiResponse> update(@PathVariable String id, @RequestBody Mono<Author> author) {
        return author
                .map(author1 -> {
                    author1.setId(id);
                    return author1;
                })
                .flatMap(repository::update)
                .map(authorUpdated ->  new ApiResponse(authorUpdated, "Author successfully updated"));
    }
    @DeleteMapping("/{id}")
    public Mono<ApiResponse> update(@PathVariable String id) {
        return repository.delete(id)
                .map(authorDeleted ->  new ApiResponse(authorDeleted, "Author successfully deleted"));
    }
}
