package io.yaade.vm.docker.manager.model.requests;

import java.util.UUID;

public record CreateVMRequest(
    UUID instanceId,
    String yaadeAdminUser,
    String yaadeAdminPwd) {

    public String vmName() {
        return "yaade-" + instanceId.toString();
    }
}
