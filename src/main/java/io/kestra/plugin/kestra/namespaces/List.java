package io.kestra.plugin.kestra.namespaces;

import io.kestra.api.sdk.KestraClient;
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
    @Schema(title = "The namespace prefix, if null, all namespaces will be listed.")
    private Property<String> prefix;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        String ns = runContext.render(prefix).as(String.class).orElse("");
        String tId = runContext.render(tenantId).as(String.class).orElse("main");


        KestraClient kestraClient = kestraClient(runContext);
        java.util.List<String> results = kestraClient.flows().listDistinctNamespaces(tId, ns);
        return Output.builder()
            .namespaces(results)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private java.util.List<String> namespaces;
    }
}
