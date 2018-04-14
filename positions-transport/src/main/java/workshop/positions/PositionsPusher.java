package workshop.positions;

import static java.util.logging.Level.SEVERE;
import static workshop.shared.Constants.DATAGRID_HOST;
import static workshop.shared.Constants.DATAGRID_PORT;
import static workshop.shared.Constants.POSITIONS_TRANSPORT_URI;
import static workshop.shared.Constants.TRAIN_POSITIONS_CACHE_NAME;
import static workshop.shared.Constants.TRAIN_POSITIONS_TOPIC;

import java.util.Collections;
import java.util.Objects;
import java.util.logging.Logger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;

import hu.akarnokd.rxjava2.interop.CompletableInterop;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.FlowableHelper;
import io.vertx.reactivex.config.ConfigRetriever;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import workshop.model.GeoLocBearing;
import workshop.model.TimedPosition;
import workshop.model.TrainPosition;

/**
 * Listens Kafka and inserts data in Infinispan
 */
public class PositionsPusher extends AbstractVerticle {

  private static final Logger log = Logger.getLogger(PositionsPusher.class.getName());

  private RemoteCacheManager client;
  private RemoteCache<String, TrainPosition> cache;

  @Override
  public void start(Future<Void> future) {
    Router router = Router.router(vertx);
    router.get(POSITIONS_TRANSPORT_URI).handler(this::consumeAndPushToCache);

    retrieveConfiguration()
      .doOnSuccess(json ->
        consumer = KafkaReadStream.create(vertx.getDelegate(), json.getJsonObject("kafka").getMap(), String.class, String.class))
      .flatMapCompletable(v ->
        vertx.createHttpServer()
          .requestHandler(router::accept)
          .rxListen(8080)
          .doOnSuccess(server -> log.info("Positions transport HTTP server started"))
          .doOnError(t -> log.log(SEVERE, "Positions transport HTTP server failed to start", t))
          .toCompletable() // Ignore result
      )
      .subscribe(CompletableHelper.toObserver(future));
  }

  private void consumeAndPushToCache(RoutingContext ctx) {
    vertx.<Infinispan>rxExecuteBlocking(fut -> fut.complete(createClient()))
      .doOnSuccess(infinispan -> {
        log.info("Connected to Infinispan");
        client = infinispan.remoteClient;
        cache = infinispan.cache;

        FlowableHelper.toFlowable(consumer).
          map(e -> CompletableInterop.fromFuture(cache.putAsync(e.key(), TrainPosition.make(e.value()))))
          .to(flowable -> Completable.merge(flowable, 100))
          .subscribe(() -> {}, t -> log.log(SEVERE, "Error while loading", t));

        consumer.subscribe(Collections.singleton(TRAIN_POSITIONS_TOPIC), ar -> {
          if (ar.succeeded()) {
            log.info("Subscription correct to " + TRAIN_POSITIONS_TOPIC);
          } else {
            log.log(SEVERE, "Could not connect to the topic", ar.cause());
          }
        });
      }).subscribe();

    ctx.response().end("Transporter started");
  }

  private Single<JsonObject> retrieveConfiguration() {
    ConfigStoreOptions store = new ConfigStoreOptions()
      .setType("file")
      .setFormat("yaml")
      .setConfig(new JsonObject()
        .put("path", "app-config.yaml")
      );
    return ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(store)).rxGetConfig();
  }

  @Override
  public void stop() {
    if (Objects.nonNull(consumer)) {
      consumer.close();
    }
    if (Objects.nonNull(client)) {
      client.stopAsync();
    }
  }

  public static class Infinispan {
    final RemoteCacheManager remoteClient;
    final RemoteCache<String, TrainPosition> cache;

    public Infinispan(RemoteCacheManager remoteClient, RemoteCache<String, TrainPosition> cache) {
      this.remoteClient = remoteClient;
      this.cache = cache;
    }
  }

  private static Infinispan createClient() {
    try {
      RemoteCacheManager client = new RemoteCacheManager(
        new ConfigurationBuilder().addServer()
          .host(DATAGRID_HOST)
          .port(DATAGRID_PORT)
          .marshaller(ProtoStreamMarshaller.class)
          .build());

      SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(client);

      ctx.registerProtoFiles(FileDescriptorSource.fromResources("train-position.proto"));
      ctx.registerMarshaller(new TrainPosition.Marshaller());
      ctx.registerMarshaller(new TimedPosition.Marshaller());
      ctx.registerMarshaller(new GeoLocBearing.Marshaller());
      return new Infinispan(client, client.getCache(TRAIN_POSITIONS_CACHE_NAME));

    } catch (Exception e) {
      log.log(SEVERE, "Error creating client", e);
      throw new RuntimeException(e);
    }
  }
}
