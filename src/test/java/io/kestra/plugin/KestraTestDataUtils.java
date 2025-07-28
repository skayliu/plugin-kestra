package io.kestra.plugin;

import io.kestra.sdk.KestraClient;
import io.kestra.sdk.internal.ApiException;
import io.kestra.sdk.model.*;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j; // Added for logging

import java.util.Random;
import java.util.UUID;

/**
 * A helper class for Kestra SDK operations for test setup and data generation.
 * This class manages its own KestraClient instance based on provided connection details.
 */
@Slf4j
public class KestraTestDataUtils {
    private final String NAMESPACE = "default";
    private final String tenantId;
    @Getter
    private final KestraClient kestraClient;
    private static final Random random = new Random();

    public KestraTestDataUtils(String kestraUrl, String username, String password, String tenantId) {
        this.tenantId = tenantId;

        var builder = KestraClient.builder();
        builder.url(kestraUrl);
        builder.basicAuth(username, password);
        this.kestraClient = builder.build();
        log.debug("KestraSDKHelper initialized with URL: {} and tenant ID: {}", kestraUrl, tenantId);
    }


    public FlowWithSource createRandomizedFlow(@Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String flow =
            """
                id: random_flow_%s
                namespace: %s

                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: Hello from KestraSDKHelper! 🚀
                """.formatted(UUID.randomUUID().toString().substring(0, 8).replace("-", "_"), np);

        return kestraClient.flows().createFlow(tenantId, flow);
    }


    public FlowWithSource createRandomizedFlowWithLabel(@Nullable String namespace) throws ApiException {
        String np = namespace != null ? namespace : "default";
        String flow =
            """
                id: random_flow_%s
                namespace: %s
                labels:
                  - key: value
                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.log.Log
                    message: Hello from KestraSDKHelper! 🚀
                """.formatted(UUID.randomUUID().toString().substring(0, 8).replace("-", "_"), np);

        return kestraClient.flows().createFlow(tenantId, flow);
    }

    public Namespace createRandomizedNamespace(@Nullable String namespaceId) throws ApiException {
        String nId = "namespace-" + UUID.randomUUID().toString().substring(0, 8).replace("-", "_");
        Namespace namespace = new Namespace().id(namespaceId != null ? namespaceId : nId);

        return kestraClient.namespaces().createNamespace(tenantId, namespace);
    }

    public void createRandomizedKVEntry(@Nullable String key, @Nullable String value, @Nullable String namespace) throws ApiException {
        kestraClient.kv().setKeyValue(
            namespace != null ? namespace : NAMESPACE,
            key != null ? key : "key-" + UUID.randomUUID().toString().substring(0, 8).replace("-", "_"),
            tenantId,
            value != null ? value : "value-" + random.nextInt(10000)
        );
    }

}
