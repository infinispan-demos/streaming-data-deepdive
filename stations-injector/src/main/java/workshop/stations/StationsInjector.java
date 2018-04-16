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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import hu.akarnokd.rxjava2.interop.CompletableInterop;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
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

  private long progressTimer;
  private Disposable injector;

  // TODO live coding

  @Override
  public void start(io.vertx.core.Future<Void> future) {
    Router router = Router.router(vertx);
    router.get(STATIONS_INJECTOR_URI).handler(this::inject);

    // TODO live coding
    vertx
      .createHttpServer()
      .requestHandler(router::accept)
      .rxListen(8080)
      .subscribe(
        server -> {
          log.info("Station injector HTTP server started");
          future.complete();
        },
        future::fail
      );
  }

  // TODO live coding - stop()

  private void inject(RoutingContext ctx) {
    if (injector != null) {
      injector.dispose();
      vertx.cancelTimer(progressTimer);
    }

    executeInject(ctx);
  }

  private void executeInject(RoutingContext ctx) {
    String fileName = "cff-stop-2016-02-29__.jsonl.gz";

    // TODO live coding
    // Station boards cache
    // Clear cache
    // Run on Vert.x context
    // Subscribe

    // TODO live coding
    // Map to key/value pair
    // Consume 1 entry each N ms, for throttling and better viewing experience
    // Dispatch each element
    // Control concurrency for cache (2nd part)

    ctx.response().end("TODO");
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

  private Completable dispatch(Map.Entry<String, Stop> entry) {
    log.info("Entry read " + entry.getKey());

    // TODO live coding

    return Completable.complete();
  }

}
