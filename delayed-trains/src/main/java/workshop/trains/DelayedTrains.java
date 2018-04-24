package workshop.trains;

import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Future;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.sockjs.SockJSHandler;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import workshop.model.GeoLocBearing;
import workshop.model.TimedPosition;
import workshop.model.TrainPosition;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static workshop.shared.Constants.DATAGRID_HOST;
import static workshop.shared.Constants.DATAGRID_PORT;
import static workshop.shared.Constants.DELAYED_TRAINS_CACHE_NAME;
import static workshop.shared.Constants.DELAYED_TRAINS_POSITIONS_ADDRESS;
import static workshop.shared.Constants.DELAYED_TRAINS_POSITIONS_URI;
import static workshop.shared.Constants.LISTEN_URI;
import static workshop.shared.Constants.TRAIN_POSITIONS_CACHE_NAME;

public class DelayedTrains extends AbstractVerticle {

  private static final Logger log = Logger.getLogger(DelayedTrains.class.getName());

  private RemoteCacheManager remote;

  private ConcurrentMap<String, String> trainIds = new ConcurrentHashMap<>();
  private DelayedTrainListener listener;
  private Long publishTimer;

  @Override
  public void start(io.vertx.core.Future<Void> future) {
    Router router = Router.router(vertx);

    router.get(DELAYED_TRAINS_POSITIONS_URI).blockingHandler(this::positionsHandler);

    // TODO live coding - sockjs and permit

    router.get(LISTEN_URI).handler(this::listen);

    vertx
      .rxExecuteBlocking(this::remoteCacheManager)
      .flatMap(remote ->
        vertx.createHttpServer()
          .requestHandler(router::accept)
          .rxListen(8080)
      )
      .subscribe(
        server -> {
          log.info("Delayed trains HTTP server started");
          future.complete();
        },
        future::fail
      );
  }

  private void listen(RoutingContext ctx) {
    vertx
      .rxExecuteBlocking(this::addDelayedTrainsListener)
      .doOnSuccess(v -> {
        if (publishTimer != null)
          vertx.cancelTimer(publishTimer);

        publishTimer = vertx.setPeriodic(3000, l -> publishPositions());
      })
      .subscribe(res ->
          ctx.response().end("Listener started")
        , t -> {
          log.log(Level.SEVERE, "Failed to start listener", t);
          ctx.response().end("Failed to start listener");
        });
  }

  private void publishPositions() {
    vertx
      .rxExecuteBlocking(this::positions)
      .subscribe(
        positions -> {
          log.info("Publishing positions:");
          log.info(positions);

          // TODO live coding
          // Publish positions
          // ...
        }
      );
  }

  private void addDelayedTrainsListener(Future<Void> f) {
    RemoteCache<Object, Object> delayed = remote.getCache(DELAYED_TRAINS_CACHE_NAME);
    if (listener != null) {
      delayed.removeClientListener(listener);
      delayed.clear();
    }

    listener = new DelayedTrainListener();
    trainIds = new ConcurrentHashMap<>();

    delayed.addClientListener(new DelayedTrainListener());

    log.info("Added delayed train listener");
    f.complete();
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

  private void positionsHandler(RoutingContext ctx) {
    log.info(() -> "HTTP GET " + DELAYED_TRAINS_POSITIONS_URI);
    ctx.response()
      .putHeader("Access-Control-Allow-Origin", "*")
      .end(showPositions());
  }

  private void positions(Future<String> f) {
    f.complete(showPositions());
  }

  private String showPositions() {
    return
      "train_id\ttrain_category\ttrain_name\ttrain_lastStopName\tposition_lat\tposition_lng\tposition_bearing\n" +
        showTrains(trainIds);
  }

  private String showTrains(ConcurrentMap<String, String> trainIds) {
    RemoteCache<String, TrainPosition> positions = remote.getCache(TRAIN_POSITIONS_CACHE_NAME);
    return trainIds.entrySet().stream()
      .map(e -> getTrainId(e, positions))
      .filter(Objects::nonNull)
      .map(positions::get)
      .map(pos -> String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s",
        pos.getTrainId(), pos.getCat(), pos.getName(), pos.getLastStopName(),
        pos.current.position.lat, pos.current.position.lng, pos.current.position.bearing
      ))
      .collect(Collectors.joining("\n"));
  }

  private String getTrainId(Map.Entry<String, String> entry, RemoteCache<String, TrainPosition> positionsCache) {
    if (!entry.getValue().isEmpty())
      return entry.getValue();

    String trainName = entry.getKey();
    QueryFactory queryFactory = Search.getQueryFactory(positionsCache);

    final String queryString = "select tp.trainId from workshop.model.TrainPosition tp where name = :trainName";

    // TODO live coding
    // Create query
    // Set query parameter
    // Get query result set
    // Get first train id (not totally accurate)

    return null;
  }

  void remoteCacheManager(Future<Void> f) {
    this.remote = new RemoteCacheManager(
      new ConfigurationBuilder().addServer()
        .host(DATAGRID_HOST)
        .port(DATAGRID_PORT)
        .marshaller(ProtoStreamMarshaller.class)
        .build());

    SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(remote);
    try {
      ctx.registerProtoFiles(FileDescriptorSource.fromResources("train-position.proto"));
      ctx.registerMarshaller(new TrainPosition.Marshaller());
      ctx.registerMarshaller(new TimedPosition.Marshaller());
      ctx.registerMarshaller(new GeoLocBearing.Marshaller());

      f.complete();
    } catch (IOException e) {
      f.fail(e);
    }
  }

  @ClientListener
  public final class DelayedTrainListener {

    @ClientCacheEntryCreated
    @SuppressWarnings("unused")
    public void created(ClientCacheEntryCreatedEvent<String> e) {
      log.info("Created event: " + e);
      String trainName = e.getKey();
      trainIds.put(trainName, "");
    }

  }

}
