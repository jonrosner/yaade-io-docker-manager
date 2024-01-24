package io.yaade.vm.docker.manager.adapters;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import io.yaade.vm.docker.manager.model.requests.CreateVMRequest;

public class DockerAdapter {

    private final Map<String, Container> containers = new ConcurrentHashMap<String, Container>();
    // clients block -> use a pool of clients
    private ArrayBlockingQueue<DockerClient> clients;
    // image of yaade
    private transient Image image;
    private String imageName;
    private int containerPort;

    public DockerAdapter(String imageName,
        int maxDockerClients, int containerPort) {
        this.imageName = imageName;
        this.containerPort = containerPort;
        this.clients = new ArrayBlockingQueue<>(maxDockerClients);

        for (int i = 0; i < maxDockerClients; i++) {
            this.clients.add(newClient());
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

    public void initDocker() throws Exception {
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

    public void startContainer(String name, Container container) throws Exception {
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

    public boolean isRunning(String name) throws Exception {
        Optional<Container> container = getContainerByName(name);
        if (container.isPresent()) {
            containers.put(name, container.get());
            return container.get().getState().equals("running");
        }

        return false;
    }

    public Container getOrCreateContainer(CreateVMRequest req)
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

    public Optional<Container> getContainerById(String cid, DockerClient docker) {
        return docker
            .listContainersCmd()
            .withShowAll(true)
            .exec()
            .stream()
            .filter(c -> c.getId().equals(cid))
            .findFirst();
    }

    public Optional<Container> getContainerByName(String name) throws Exception {
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

    public int countRunningContainers() throws Exception {
        DockerClient docker = this.clients.take();
        try {
            return docker.listContainersCmd().withShowAll(true).exec().stream()
                .filter(c -> c.getImageId().equals(image.getId()))
                .filter(c -> c.getState().equals("running")).collect(Collectors.toList())
                .size();
        } finally {
            clients.add(docker);
        }
    }
}
