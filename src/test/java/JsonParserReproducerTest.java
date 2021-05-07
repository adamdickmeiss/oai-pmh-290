import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.vertx.core.http.HttpHeaders.ACCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JsonParserReproducerTest {

  private static final int REQUEST_TIMEOUT = 904800000;
  private static final int ROWS_NUMBER = 100000;
  private static final int THREAD_NUM = 5;
  private static final int MOCK_SERVER_PORT = 7575;
  private static final String STREAM_PRODUCER_URI = "/producer";
  private static final String responseBodyTemplate = "{ \"instanceId\":\"1ed91465-7a75-4d96-bf34-4dfbd89790d5\", \"source\":\"GLOBAL\", \"updatedDate\":\"2020-06-15T11:07:48.563Z\", \"deleted\":\"false\", \"suppressFromDiscovery\":\"false\"}";
  private static ExecutorService executorService;
  private static boolean threadsStarted;

  @BeforeAll
  void setUp(Vertx vertx, VertxTestContext testContext) {
    startMockServer(vertx, testContext);
    executorService = Executors.newFixedThreadPool(THREAD_NUM);
  }

@AfterAll
  void tearDown(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
    threadsStarted = false;
    executorService.awaitTermination(1, TimeUnit.SECONDS);
    vertx.close().onSuccess(v -> testContext.completeNow())
        .onFailure(testContext::failNow);
  }

  @Test
  void shouldNotThrowJsonParseException_whenProcessingStreamInAsyncMode(Vertx vertx,
      VertxTestContext testContext) {

    threadsStarted = true;

    WebClient client = WebClient.create(vertx);
    JsonParser parser = JsonParser.newParser().objectValueMode();
    AtomicInteger recordsCount = new AtomicInteger();
    parser.handler(event -> {
      if (recordsCount.incrementAndGet() % 50 == 0) {
        System.out.println("Records returned so far: " + recordsCount.get());
      }
      if (recordsCount.get() == ROWS_NUMBER) {
        testContext.completeNow();
      }
    });
    parser.exceptionHandler(testContext::failNow);

    AtomicBoolean isPaused = new AtomicBoolean();
    // Creates jobs that imitates the work with JsonParser in asynchronous way
    ReentrantLock lock = new ReentrantLock();
    for (int i = 0; i < THREAD_NUM; i++) {
      executorService.submit(() -> {
        while (threadsStarted) {
          lock.lock();
          if (isPaused.get()) {
            parser.resume();
          } else {
            parser.pause();
            isPaused.set(true);
          }
          lock.unlock();
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }

    client.getAbs("http://localhost:" + MOCK_SERVER_PORT + STREAM_PRODUCER_URI)
        .putHeader(ACCEPT.toString(), APPLICATION_JSON.toString())
        .timeout(REQUEST_TIMEOUT)
        .as(BodyCodec.jsonStream(parser))
        .send()
        .onFailure(testContext::failNow);
  }

  /**
   * Creates and starts the mock server with defined router for producing json entity stream.
   */
  private void startMockServer(Vertx vertx, final VertxTestContext testContext) {
    HttpServer mockServer = vertx.createHttpServer();
    mockServer.requestHandler(setupStreamProducer(vertx))
        .listen(MOCK_SERVER_PORT, asyncResult -> {
          if (asyncResult.succeeded()) {
            testContext.completeNow();
          } else {
            testContext.failNow(asyncResult.cause());
          }
        });
  }

  private Router setupStreamProducer(Vertx vertx) {
    Router router = Router.router(vertx);
    router.get(STREAM_PRODUCER_URI)
        .handler(this::handleStreamProducerGetResponse);
    return router;
  }

  private void handleStreamProducerGetResponse(RoutingContext routingContext) {
    HttpServerResponse response = routingContext.response();
    response.setStatusCode(200);
    response.putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
    response.setChunked(true);
    int rows = ROWS_NUMBER;
    while (rows > 0) {
      response.write(responseBodyTemplate);
      rows--;
    }
    response.end();
  }

}
