package workshop.stations;

import static java.util.logging.Level.SEVERE;
import static workshop.shared.Constants.DATAGRID_HOST;
import static workshop.shared.Constants.DATAGRID_PORT;
import static workshop.shared.Constants.STATIONS_INJECTOR_URI;
import static workshop.shared.Constants.STATION_BOARDS_CACHE_NAME;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import hu.akarnokd.rxjava2.interop.CompletableInterop;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Future;
import io.vertx.reactivex.core.RxHelper;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import workshop.model.Station;
import workshop.model.Stop;
import workshop.model.Train;

public class StationsInjector extends AbstractVerticle {

  private static final Logger log = Logger.getLogger(StationsInjector.class.getName());

  private RemoteCacheManager remote;
  private RemoteCache<String, Stop> stationBoardsCache;

  @Override
  public void start(io.vertx.core.Future<Void> future) {
    Router router = Router.router(vertx);
    router.get(STATIONS_INJECTOR_URI).handler(this::inject);

    vertx
      .rxExecuteBlocking(this::remoteCacheManager)
      .flatMap(x ->
        vertx
          .createHttpServer()
          .requestHandler(router::accept)
          .rxListen(8080)
      )
      .subscribe(
        server -> {
          log.info("Http server started and connected to datagrid");
          future.complete();
        }
        , future::fail
      );
  }

  @Override
  public void stop(io.vertx.core.Future<Void> future) {
    if (Objects.nonNull(remote)) {
      remote.stopAsync()
        .thenRun(future::complete);
    } else {
      future.complete();
    }
  }

  private void inject(RoutingContext ctx) {
    vertx
      .rxExecuteBlocking(stationBoardsCache())
      .flatMapCompletable(x -> clearStationBoardsCache())
      .subscribeOn(RxHelper.scheduler(vertx.getOrCreateContext()))
      .subscribe(() -> {
        vertx.setPeriodic(5000L, l ->
          vertx.executeBlocking(fut -> {
            log.info(String.format("Progress: stored=%d%n", stationBoardsCache.size()));
            fut.complete();
          }, false, ar -> {}));

        Flowable<String> fileFlowable = rxReadGunzippedTextResource("cff-stop-2016-02-29__.jsonl.gz");

        Flowable<Map.Entry<String, Stop>> pairFlowable = fileFlowable.map(StationsInjector::toEntry);

        Completable completable = pairFlowable.map(e -> {
          CompletableFuture<Stop> putCompletableFuture =
              stationBoardsCache.putAsync(e.getKey(), e.getValue());
          return CompletableInterop.fromFuture(putCompletableFuture);
        }).to(flowable -> Completable.merge(flowable, 100));

        completable.subscribe(() -> {}, t -> log.log(SEVERE, "Error while loading", t));

        ctx.response().end("Injector started");
      });

//    Flowable<String> fileFlowable = rxReadGunzippedTextResource("cff-stop-2016-02-29__.jsonl.gz");
//    fileFlowable
//      .map(StationsInjector::toEntry)
//      .flatMapCompletable(this::dispatch)
//      .subscribeOn(Schedulers.io())
//      .doOnError(t -> log.log(SEVERE, "Error while loading", t))
//      .subscribe();
//    ctx.response().end("Injector started");
  }

  private Completable clearStationBoardsCache() {
    return CompletableInterop.fromFuture(stationBoardsCache.clearAsync());
  }

  // TODO: Duplicate
  private static Flowable<String> rxReadGunzippedTextResource(String resource) {
    Objects.requireNonNull(resource);
    URL url = StationsInjector.class.getClassLoader().getResource(resource);
    Objects.requireNonNull(url);

    return Flowable.<String, BufferedReader>generate(() -> {
      InputStream inputStream = url.openStream();
      InputStream gzipStream = new GZIPInputStream(inputStream);
      Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
      return new BufferedReader(decoder);
    }, (bufferedReader, emitter) -> {
      String line = bufferedReader.readLine();
      if (line != null) {
        emitter.onNext(line);
      } else {
        emitter.onComplete();
      }
    }, BufferedReader::close)
      .subscribeOn(Schedulers.io());
  }

  private static Map.Entry<String, Stop> toEntry(String line) {
    JsonObject json = new JsonObject(line);
    String trainName = json.getString("name");
    String trainTo = json.getString("to");

    JsonObject jsonStop = json.getJsonObject("stop");
    JsonObject jsonStation = jsonStop.getJsonObject("station");
    long stationId = Long.parseLong(jsonStation.getString("id"));

    String departure = jsonStop.getString("departure");
    String stopId = String.format("%s/%s/%s/%s", stationId, trainName, trainTo, departure);

    return new AbstractMap.SimpleImmutableEntry<>(stopId, Stop.make(line));
  }

  private Completable dispatch(Map.Entry<String, String> e) {
    CompletableFuture<Stop> future =
      stationBoardsCache.putAsync(e.getKey(), Stop.make(e.getValue()));

    return CompletableInterop.fromFuture(future);
//    ProducerRecord<String, String> record
//      = new ProducerRecord<>(STATION_BOARDS_TOPIC, entry.getKey(), entry.getValue());
//    return new AsyncResultCompletable(
//      handler ->
//        stream.write(record, x -> {
//          if (x.succeeded()) {
//            log.info("Entry written in Kafka: " + entry.getKey());
//            handler.handle(Future.succeededFuture());
//          } else {
//            handler.handle(Future.failedFuture(x.cause()));
//          }
//        }));
  }

  private void remoteCacheManager(Future<Void> f) {
    try {
      remote = new RemoteCacheManager(
        new ConfigurationBuilder().addServer()
          .host(DATAGRID_HOST)
          .port(DATAGRID_PORT)
          .marshaller(ProtoStreamMarshaller.class)
          .build());

      SerializationContext ctx =
        ProtoStreamMarshaller.getSerializationContext(remote);

      ctx.registerProtoFiles(
        FileDescriptorSource.fromResources("station-board.proto")
      );

      ctx.registerMarshaller(new Stop.Marshaller());
      ctx.registerMarshaller(new Station.Marshaller());
      ctx.registerMarshaller(new Train.Marshaller());

      f.complete();
    } catch (Exception e) {
      log.log(Level.SEVERE, "Error creating client", e);
      throw new RuntimeException(e);
    }
  }

  private Handler<Future<Void>> stationBoardsCache() {
    return f -> {
      this.stationBoardsCache = remote.getCache(STATION_BOARDS_CACHE_NAME);
      f.complete();
    };
  }

}
