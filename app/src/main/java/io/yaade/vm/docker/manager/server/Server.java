package io.yaade.vm.docker.manager.server;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports.Binding;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
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

	// clients block -> use a pool of clients
	private ArrayBlockingQueue<DockerClient> clients;
	// image of yaade
	private transient Image image;
	// available yaade containers
	private final Map<String, Container> containers = new ConcurrentHashMap<String, Container>();

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
			this.clients = new ArrayBlockingQueue<>(maxDockerClients);

			for (int i = 0; i < maxDockerClients; i++) {
				clients.add(newClient());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		vertx.executeBlocking(() -> {
			initDocker();
			return null;
		}).compose(r -> {
			// start web server
			HttpServer server = vertx.createHttpServer();
			Router router = Router.router(vertx);

			// allow connections only from gateway host
			router.route().handler(ctx -> firewall(ctx));
			router.post("/init").blockingHandler(ctx -> {
				try {
					initDocker();
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
			server.requestHandler(router);
			return server.listen(port);
		}).onSuccess(s -> System.out.println("listening on " + port))
			.onFailure(e -> {
				e.printStackTrace();
			});
	}

	private void initDocker() throws Exception {
		// init yaade image
		DockerClient docker = clients.take();
		docker.pingCmd().exec();
		List<Image> images = docker.listImagesCmd().exec();
		this.image = null;
		if (images != null)
			images = images.stream()
				.filter(image -> image.getRepoTags() != null
					&& !Arrays.stream(image.getRepoTags())
						.filter(tag -> tag.equals(imageName))
						.collect(Collectors.toList()).isEmpty())
				.collect(Collectors.toList());
		if (images == null || images.isEmpty())
			throw new RuntimeException("image not found: " + imageName);
		System.out.println("found images: " + images.size());
		this.image = images.get(0);

		// init existing containers
		List<Container> containers = docker.listContainersCmd().withShowAll(true).exec();
		containers = containers.stream().filter(c -> c.getImageId().equals(image.getId()))
			.collect(Collectors.toList());
		System.out.println("found containers: " + containers.size());
		for (Container container : containers)
			this.containers.put(container.getNames()[0].substring(1), container);
		// return client to pool
		clients.add(docker);
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

	private DockerClient newClient() {
		DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
			.build();
		DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
			.dockerHost(config.getDockerHost())
			.sslConfig(config.getSSLConfig())
			// .maxConnections(100)
			.connectionTimeout(Duration.ofSeconds(2))
			// .responseTimeout(Duration.ofSeconds(45))
			.build();

		DockerClient result = DockerClientImpl.getInstance(config, httpClient);
		return result;
	}

	private void startContainer(String name, Container container) throws Exception {
		if (isRunning(name)) {
			return;
		}
		System.out.println("+startContainer " + name);
		DockerClient docker = clients.take();
		try {
			String containerId = container.getId();
			if (containerId == null) {
				throw new RuntimeException("container not found: " + name);
			}

			docker.startContainerCmd(containerId).exec();
			System.out.println("-startContainer " + name);
		} finally {
			clients.add(docker);
		}
	}

	private boolean isRunning(String name) throws Exception {
		Optional<Container> container = getContainerByName(name);
		if (container.isPresent()) {
			containers.put(name, container.get());
			return container.get().getState().equals("running");
		}

		return false;
	}

	private Container getOrCreateContainer(CreateVMRequest req)
		throws Exception {
		Container result = containers.get(req.vmName());
		if (result != null) {
			return result;
		}
		DockerClient docker = clients.take();
		try {
			HostConfig hostConfig = HostConfig.newHostConfig()
				.withBinds(new Bind("/app/data", new Volume("/" + req.vmName())))
				.withPortBindings(
					new PortBinding(Binding.bindPort(0),
						new ExposedPort(containerPort)));

			String imageId = image.getId();
			if (imageId == null) {
				throw new RuntimeException("image not found: " + imageName);
			}

			CreateContainerResponse cc = docker.createContainerCmd(imageId)
				.withHostConfig(hostConfig)
				.withEnv("YAADE_ADMIN_USERNAME=" + req.yaadeAdminUser(),
					"YAADE_ADMIN_PASSWORD=" + req.yaadeAdminPwd())
				.withName(req.vmName())
				.exec();

			String cid = cc.getId();
			result = getContainerById(cid, docker)
				.orElseThrow(() -> new RuntimeException("container not found: " + cid));
			containers.put(req.vmName(), result);

			return result;
		} finally {
			clients.add(docker);
		}
	}

	private Optional<Container> getContainerById(String cid, DockerClient docker) {
		return docker
			.listContainersCmd()
			.withShowAll(true)
			.exec()
			.stream()
			.filter(c -> c.getId().equals(cid))
			.findFirst();
	}

	private Optional<Container> getContainerByName(String name) throws Exception {
		DockerClient docker = clients.take();
		try {
			return docker
				.listContainersCmd()
				.withShowAll(true)
				.exec()
				.stream()
				.filter(
					c -> c != null && Arrays.asList(c.getNames()).contains("/" + name))
				.findFirst();
		} finally {
			clients.add(docker);
		}
	}

	private void startVM(RoutingContext ctx) {
		System.out.println("startVM");
		JsonObject body = ctx.body().asJsonObject();
		CreateVMRequest req = body.mapTo(CreateVMRequest.class);
		int port;
		try {
			Container container = getOrCreateContainer(req);
			startContainer(req.vmName(), container);
			port = container.getPorts()[0].getPublicPort();
		} catch (Exception e) {
			e.printStackTrace();
			ctx.fail(e);
			return;
		}
		try {
			Thread.sleep(yaadeStartupTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ctx.end(new JsonObject().put("port", port).encode());
	}

}
