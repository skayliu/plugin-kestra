package io.kestra.plugin.kestra.flow;

import io.kestra.api.sdk.KestraClient;
import io.kestra.api.sdk.model.Flow;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Kestra flows"
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Simple revert",
            code = { "format: \"Text to be reverted\"" }
        )
    }
)
public class List extends AbstractKestraTask implements RunnableTask<List.Output> {
    @Schema(title = "The namespace to list flows on, if null, will default to the namespace of the current flow.")
    private Property<String> namespace;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        String ns = runContext.render(namespace).as(String.class).orElseGet(() -> runContext.flowInfo().namespace());

        KestraClient kestraClient = kestraClient(runContext);
        java.util.List<Flow> flows = kestraClient.flows().getFlowsByNamespace3(ns, runContext.flowInfo().tenantId());
        return Output.builder()
            .flows(flows)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private java.util.List<Flow> flows;
    }
}
