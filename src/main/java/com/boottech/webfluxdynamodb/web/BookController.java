package com.boottech.webfluxdynamodb.web;


import com.boottech.webfluxdynamodb.domain.Book;
import com.boottech.webfluxdynamodb.repository.BookRepository;
import com.boottech.webfluxdynamodb.web.dto.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;

@RestController
@RequestMapping("/book")
public class BookController {


    private final BookRepository repository;

    @Autowired
    public BookController(BookRepository repository) {
        this.repository = repository;
    }

    @GetMapping()
    public Mono<ApiResponse> getAllBooks() {
        return repository.findAll()
                .collectList()
                .map(books -> new ApiResponse(books, MessageFormat.format("{0} result found", books.size())));
    }

    @GetMapping("/count")
    public Mono<ApiResponse> bookCount() {
        return repository.count()
                .map(count -> new ApiResponse(count, MessageFormat.format("Count books: {0}", count)));
    }

    @GetMapping("/{id}")
    public Mono<ApiResponse> getByBookId(@PathVariable String id) {
        return repository.findById(id)
                .map(book -> new ApiResponse(book, MessageFormat.format("Result found", book)))
                .defaultIfEmpty(new ApiResponse(null, "Book not found"));
    }

    @PostMapping()
    public Mono<ApiResponse> create(@RequestBody Mono<Book> book) {
        return book
                .flatMap(repository::save)
                .map(authorCreated ->  new ApiResponse(authorCreated, "Book successfully created"));
    }
    @PutMapping("/{id}")
    public Mono<ApiResponse> update(@PathVariable String id, @RequestBody Mono<Book> book) {
        return book
                .map(book1 -> {
                    book1.setId(id);
                    return book1;
                })
                .flatMap(repository::update)
                .map(bookUpdated ->  new ApiResponse(bookUpdated, "Book successfully updated"));
    }
    @DeleteMapping("/{id}")
    public Mono<ApiResponse> update(@PathVariable String id) {
        return repository.delete(id)
                .map(bookDeleted ->  new ApiResponse(bookDeleted, "Book successfully deleted"));
    }
}
