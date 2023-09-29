import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<String>> handleGlobalException(Exception ex) {
        // Handle generic exceptions here
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("An error occurred: " + ex.getMessage()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ResponseEntity<String>> handleResponseStatusException(ResponseStatusException ex) {
        // Handle ResponseStatusException (e.g., not found, bad request)
        return Mono.just(ResponseEntity.status(ex.getStatus())
                .body(ex.getReason()));
    }

    // You can add more exception handlers for specific exception types as needed

}
