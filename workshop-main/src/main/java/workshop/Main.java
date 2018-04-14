package workshop;

import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;

import java.util.logging.Level;
import java.util.logging.Logger;

import static workshop.shared.Constants.*;

public class Main extends AbstractVerticle {

  private static final Logger log = Logger.getLogger(Main.class.getName());

  @Override
  public void start(Future<Void> future) throws Exception {
    log.info("Starting Main verticle");

    Router router = Router.router(vertx);
    router.get("/inject").handler(this::inject);

    vertx.createHttpServer()
      .requestHandler(router::accept)
      .rxListen(8080)
      .flatMap(server -> vertx.rxExecuteBlocking(Admin::createRemoteCaches))
      .doOnSuccess(server -> log.info("Main HTTP server started"))
      .toCompletable() // Ignore result
      .subscribe(CompletableHelper.toObserver(future));
  }

  private void inject(RoutingContext ctx) {
    log.info("HTTP GET /inject");
    vertx
      .rxExecuteBlocking(Admin::clearCaches)
      .flatMap(x -> httpGet(DELAYED_TRAINS_HOST, LISTEN_URI))
      .flatMap(x -> httpGet(STATIONS_INJECTOR_HOST, STATIONS_INJECTOR_URI))
      .flatMap(x -> httpGet(POSITIONS_INJECTOR_HOST, POSITIONS_INJECTOR_URI))
      .subscribe(rsp -> {
        log.info("Inject replied: " + rsp.body());
        ctx.response().end("Inject OK");
      }, t -> {
        log.log(Level.SEVERE, "Error starting data injection", t);
        ctx.response().end("Failed to start data injection");
      });
  }

  private Single<HttpResponse<String>> httpGet(String host, String uri) {
    log.info("Call HTTP GET " + host + uri);
    WebClient client = WebClient.create(vertx);
    return client
      .get(8080, host, uri)
      .as(BodyCodec.string())
      .rxSend();
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(Main.class.getName());
  }

}
