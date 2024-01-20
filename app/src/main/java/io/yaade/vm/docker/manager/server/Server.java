package io.yaade.vm.docker.manager.server;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

		initDocker();

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
		server.listen(port).onSuccess(s -> System.out.println("listening on " + port))
			.onFailure(e -> e.printStackTrace());
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
		} else
			ctx.next();
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
		if (isRunning(name))
			return;
		System.out.println("+startContainer " + name);
		DockerClient docker = clients.take();

		String containerId = container.getId();
		if (containerId == null) {
			throw new RuntimeException("container not found: " + name);
		}

		docker.startContainerCmd(containerId).exec();
		clients.add(docker);
		System.out.println("-startContainer " + name);
	}

	private boolean isRunning(String name) throws Exception {
		Container container = getContainerByName(name);
		if (container != null)
			containers.put(name, container);
		return container != null && container.getState().equals("running");
	}

	private Container getOrCreateContainer(String name, JsonObject data) throws Exception {
		Container result = containers.get(name);
		if (result != null) {
			return result;
		}
		DockerClient docker = clients.take();

		HostConfig hostConfig = HostConfig.newHostConfig()
			.withBinds(new Bind("/app/data", new Volume("/" + name)))
			.withPortBindings(new PortBinding(Binding.bindPort(data.getInteger("vmPort")),
				new ExposedPort(containerPort)));

		String imageId = image.getId();
		if (imageId == null) {
			throw new RuntimeException("image not found: " + imageName);
		}

		CreateContainerResponse cc = docker.createContainerCmd(imageId)
			.withHostConfig(hostConfig)
			.withEnv("YAADE_ADMIN_USERNAME=" + data.getString("id"))
			.withName(name)
			.exec();
		clients.add(docker);

		String cid = cc.getId();
		result = getContainerById(cid);
		containers.put(name, result);

		return result;
	}

	private Container getContainerById(String cid) throws Exception {
		DockerClient docker = clients.take();
		List<Container> cs = docker.listContainersCmd().withShowAll(true).exec().stream()
			.filter(c -> c.getId().equals(cid)).collect(Collectors.toList());
		Container result = cs.get(0);
		clients.add(docker);
		return result;
	}

	private Container getContainerByName(String name) throws Exception {
		DockerClient docker = clients.take();
		final String cName = "/" + name;
		List<Container> cs = docker.listContainersCmd().withShowAll(true).exec().stream()
			.filter(
				c -> c.getNames() != null && Arrays.asList(c.getNames()).contains(cName))
			.collect(Collectors.toList());
		Container result = cs.isEmpty() ? null : cs.get(0);
		clients.add(docker);
		return result;
	}

	private void startVM(RoutingContext ctx) {
		System.out.println("startVM");
		JsonObject data = ctx.body().asJsonObject();
		String name = containerName(data.getString("id"));
		try {
			Container container = getOrCreateContainer(name, data);
			startContainer(name, container);
		} catch (Exception e) {
			e.printStackTrace();
			ctx.fail(e);
			return;
		}
		try {
			Thread.sleep(yaadeStartupTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ctx.end();
	}

	private String containerName(String id) {
		return "yaade-" + id;
	}

}
