package io.yaade.vm.docker.manager;

import io.vertx.core.Vertx;
import io.yaade.vm.docker.manager.config.Config;
import io.yaade.vm.docker.manager.server.Server;

public class App {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        Config.init(vertx)
            .onSuccess((config) -> {
                System.out.println(config);
                vertx.deployVerticle(new Server(config));
            })
            .onFailure(e -> e.printStackTrace());
    }
}
