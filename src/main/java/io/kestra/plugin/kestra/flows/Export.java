package io.kestra.plugin.kestra.flows;

import io.kestra.api.sdk.KestraClient;
import io.kestra.api.sdk.model.IdWithNamespace;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

public class Export extends AbstractKestraTask implements RunnableTask<Export.Output> {
    @Schema(title = "The namespace to list flows on, if null, will default to the namespace of the current flow.")
    public Property<List<IdWithNamespace>> idsWithNamespace;

    @Override
    public Export.Output run(RunContext runContext) throws Exception {
        String tId = runContext.render(tenantId).as(String.class).orElse("main");
        List<IdWithNamespace> ids = runContext.render(idsWithNamespace).asList(IdWithNamespace.class);

        KestraClient kestraClient = kestraClient(runContext);
        byte[] zipBytes = kestraClient.flows().exportFlowsByIds(tId, ids);

        InputStream inputStream = new ByteArrayInputStream(zipBytes);
        String fileName = "exported_flows.zip";
        URI storedFileUri = runContext.storage().putFile(inputStream, fileName);


        return Export.Output.builder()
            .flowsZip(storedFileUri)
            .build();
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private URI flowsZip;
    }
}
