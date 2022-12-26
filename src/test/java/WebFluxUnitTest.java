import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WebFluxUnitTest {
    @Test
    public void testCreateFluxAndSubscribe() {
        Flux<Integer> fluxFromJust = Flux.just(1, 2, 3).log();
        StepVerifier.create(fluxFromJust)
                .expectNext(1)
                .expectNext(2)
                .expectNext(3)
                .verifyComplete();
    }


    @Test
    public void testCreateFluxAndSubscribeVerifyError() {
        Flux<Integer> fluxFromJust = Flux.just(1).concatWith(Flux.error(new RuntimeException("test")));
        StepVerifier.create(fluxFromJust)
                .expectNextCount(1)
                .verifyError(RuntimeException.class);
    }


    //Filtering
    @Test
    public void testFilteringFlux() {
        Flux<Integer> fluxFromJust = Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).log();
        Flux<Integer> filter = fluxFromJust.filter(i -> i % 2 == 0);
        StepVerifier.create(filter)
                .expectNext(2, 4, 6, 8, 10)
                .verifyComplete();
    }

    //distinct
    @Test
    public void distinct() {
        Flux<Integer> fluxFromJust = Flux.just(1, 2, 3, 4, 5, 1, 2, 3, 4, 5).log();
        Flux<Integer> distinct = fluxFromJust.distinct();
        StepVerifier
                .create(distinct)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    //takeWhile
    @Test
    public void takewhile() {
        Flux<Integer> fluxFromJust = Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Flux<Integer> takeWhile = fluxFromJust.takeWhile(i -> i <= 5);
        StepVerifier.create(takeWhile)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    //skipwhile
    @Test
    public void skipwhile() {
        Flux<Integer> fluxFromJust = Flux.just(1, 2, 3, 4, 5, 6);
        Flux<Integer> skipWhile = fluxFromJust.skipWhile(i -> i<3);
        StepVerifier.create(skipWhile)
                .expectNext(3,4,5,6)
                .verifyComplete();
    }


    @Test
    public void testMapOperationFlux(){
        Flux<String> fluxFromJust = Flux.just("Hello", "World");
        Flux<Integer> fluxMap = fluxFromJust.map(i -> i.length());
        StepVerifier.create(fluxMap)
                .expectNext(5,5)
                .verifyComplete();
    }

    //testFlatMap
    @Test
    public void testFlatMapFlux(){
        Flux<Integer> fluxFromJust = Flux.just(1,2).log();
        Flux<Integer> integerFlux  = fluxFromJust.flatMap(i -> getSomeFlux(i));
        StepVerifier.create(integerFlux)
                .expectNextCount(20)
                .verifyComplete();
    }
    private Flux<Integer> getSomeFlux(Integer i) {
        return Flux.range(i, 10);
    }

    //Index
    @Test
    public void maintainIndex(){
        Flux<Tuple2<Long, String>> index = Flux.just("First","Second", "Third").index();
        StepVerifier.create(index)
                .expectNext(Tuples.of(0L, "First"))
                .expectNext(Tuples.of(1L,"Second"))
                .expectNext(Tuples.of(2L, "Third"))
                .verifyComplete();
    }

    //flatMapMany
    @Test
    public void flatMapOperator(){
        Mono<List<Integer>> just = Mono.just(Arrays.asList(1,2,3));
        Flux<Integer> integerFlux = just.flatMapMany(it -> Flux.fromIterable(it));
        StepVerifier.create(integerFlux)
                .expectNext(1,2,3)
                .verifyComplete();
    }

    //startwith
    @Test
    public void startWith(){
        Flux<Integer> just = Flux.just(1,2,3);
        Flux<Integer> iintegerFlux = just.startWith(0);
        StepVerifier.create(iintegerFlux)
                .expectNext(0,1,2,3)
                .verifyComplete();
    }

    //ConcatWith
    @Test
    public void concatWith(){
        Flux<Integer> just =  Flux.just(1,2,3,4,5);
        Flux<Integer> conWith = just.concatWith(Flux.just(6,7));
        StepVerifier.create(conWith)
                .expectNext(1,2,3,4,5,6,7)
                .verifyComplete();
    }

    //Merge
    @Test
    public void zip() throws InterruptedException{
        Flux<Integer> firstFlux = Flux.just(1,2,3,4,5).delayElements(Duration.ofSeconds(1));
        Flux<Integer> secondFlux = Flux.just(10,20,33,42,51).delayElements(Duration.ofSeconds(1));
        firstFlux.mergeWith(secondFlux)
                .subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(11);
    }

    //collectList & CollectSortedList
    @Test
    public void collectList(){
        Mono<List<Integer>> listMono = Flux
                .just(1,2,3)
                .collectList();
        StepVerifier.create(listMono)
                .expectNext(Arrays.asList(1,2,3))
                .verifyComplete();
    }

    @Test
    public void CollectSortedisList(){
        Mono<List<Integer>> listMono = Flux.just(1,2,3,9,8)
                .collectSortedList();
        StepVerifier.create(listMono)
                .expectNext(Arrays.asList(1,2,3,8,9))
                .verifyComplete();

    }

    //zip
    @Test
    public void zipMain(){
        Flux<Integer> firstFlux = Flux.just(1,2,3);
        Flux<Integer> secondFlux = Flux.just(10,11,12);
        Flux<Integer> zip = Flux.zip(firstFlux, secondFlux, (num1,num2) -> num1 + num2);
        StepVerifier.create(zip)
                .expectNext(11,13, 15)
                .verifyComplete();
    }

    //buffer
    @Test
    public void bufferTest(){
        Flux<List<Integer>> buffer = Flux.just(1,2,3,4,5,6,7).log()
                .buffer(2);
        StepVerifier.create(buffer)
                .expectNext(Arrays.asList(1,2))
                .expectNext(Arrays.asList(3,4))
                .expectNext(Arrays.asList(5,6))
                .expectNext(Arrays.asList(7))
                .verifyComplete();
    }
}