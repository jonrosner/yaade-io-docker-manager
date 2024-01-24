package io.yaade.vm.docker.manager.config;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Config {

	private static JsonObject config;

	public static Future<JsonObject> init(Vertx vertx) {
		ConfigRetrieverOptions options = new ConfigRetrieverOptions();
		String path = Config.class.getResource("/config.json").getPath();
		ConfigStoreOptions fileStore = new ConfigStoreOptions()
			.setType("file")
			.setConfig(new JsonObject().put("path", path));
		options.addStore(fileStore);
		ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
		Promise<JsonObject> promise = Promise.promise();
		retriever.getConfig().onComplete(ar -> {
			if (ar.failed()) {
				promise.fail(ar.cause());
			} else {
				config = ar.result();
				System.out.println("config loaded");
				promise.complete(config);
			}
		});
		return promise.future();
	}

	public static JsonObject config() {
		return config;
	}
}
