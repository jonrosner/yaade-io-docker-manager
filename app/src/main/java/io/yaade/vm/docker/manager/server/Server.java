package io.yaade.vm.docker.manager.server;

import java.util.Set;
import java.util.stream.Collectors;

import com.github.dockerjava.api.model.Container;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.yaade.vm.docker.manager.adapters.DockerAdapter;
import io.yaade.vm.docker.manager.model.requests.CreateVMRequest;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Server extends AbstractVerticle {

	private final JsonObject config;

	public int port;
	private int maxDockerClients;
	private Set<String> allowedIps;
	private String imageName;
	private long yaadeStartupTime;

	private int containerPort;

	private DockerAdapter dockerAdapter;

	@Override
	public void start() throws Exception {
		try {
			this.port = config.getInteger("port");
			this.maxDockerClients = config.getInteger("max_docker_clients");
			this.allowedIps = config.getJsonArray("allowed_ips").stream()
				.map(Object::toString)
				.collect(Collectors.toSet());
			this.imageName = config.getString("image_name");
			this.yaadeStartupTime = config.getLong("yaade_startup_time");
			this.containerPort = config.getInteger("container_port");

			this.dockerAdapter = new DockerAdapter(imageName, maxDockerClients,
				containerPort);

			vertx.executeBlocking(() -> {
				this.dockerAdapter.initDocker();
				return null;
			}).compose(r -> {
				// start web server
				HttpServer server = vertx.createHttpServer();
				Router router = Router.router(vertx);

				// allow connections only from gateway host
				// router.route().handler(ctx -> firewall(ctx));
				router.post("/init").blockingHandler(ctx -> {
					try {
						this.dockerAdapter.initDocker();
						ctx.end();
					} catch (Exception e) {
						e.printStackTrace();
						ctx.fail(e);
					}
				});
				router.route().handler(BodyHandler.create());
				router.post("/start").blockingHandler(ctx -> {
					startVM(ctx);
				});
				router.post("/stop").blockingHandler(ctx -> {
					// stopVM(ctx);
				});
				router.get("/has-capacity").blockingHandler(this::checkCapacity);
				server.requestHandler(router);
				return server.listen(port);
			}).onSuccess(s -> System.out.println("listening on " + port))
				.onFailure(e -> {
					e.printStackTrace();
				});
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void firewall(RoutingContext ctx) {
		String remoteIp = ctx.request().remoteAddress().hostAddress();
		if (!allowedIps.contains(remoteIp)) {
			System.out.println("firewall: bad remote: " + remoteIp);
			ctx.fail(401);
		} else {
			ctx.next();
		}
	}

	private void startVM(RoutingContext ctx) {
		System.out.println("startVM");
		JsonObject body = ctx.body().asJsonObject();
		CreateVMRequest req = body.mapTo(CreateVMRequest.class);
		int port;
		try {
			Container container = this.dockerAdapter.getOrCreateContainer(req);
			this.dockerAdapter.startContainer(req.vmName(), container);
			port = container.getPorts()[0].getPublicPort();
			Thread.sleep(yaadeStartupTime);
		} catch (Exception e) {
			e.printStackTrace();
			ctx.fail(e);
			return;
		}
		ctx.end(new JsonObject().put("port", port).encode());
	}

	private void checkCapacity(RoutingContext ctx) {
		try {
			if (!hasCapacity()) {
				ctx.fail(400);
				return;
			}
			ctx.end();
		} catch (Exception e) {
			e.printStackTrace();
			ctx.fail(e);
		}
	}

	private boolean hasCapacity() throws Exception {
		int numRunning = this.dockerAdapter.countRunningContainers();
		int maxRunning = config.getInteger("max_vms");
		return numRunning < maxRunning;
	}

}
