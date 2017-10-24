package no.nav.opptjening.hiv.hendelser.batch;

import no.nav.opptjening.hiv.hendelser.support.BatchLoggService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;

import java.util.Map;

public class ItemChunkListener implements ChunkListener {
    private static final Logger LOG = LoggerFactory.getLogger(ItemChunkListener.class);

    private BatchLoggService service;

    public ItemChunkListener(BatchLoggService service) {
        this.service = service;
    }

    @Override
    public void beforeChunk(ChunkContext context) {
    }

    @Override
    public void afterChunk(ChunkContext context) {
        StepContext stepContext = context.getStepContext();
        StepExecution stepExecution = stepContext.getStepExecution();

        long jobId = stepExecution.getJobExecution().getJobId();

        LOG.info("COMMIT_COUNT={}, READ_COUNT={}, WRITE_COUNT={}",
                stepExecution.getCommitCount(), stepExecution.getReadCount(), stepExecution.getWriteCount());

        Map<String, Object> executionContext = stepContext.getStepExecutionContext();
        long fraSekvensnummer = (long)executionContext.get("fra_sekvensnummer");
        long tilSekvensnummer = fraSekvensnummer;

        if (executionContext.containsKey("til_sekvensnummer")) {
            tilSekvensnummer = (long) executionContext.get("til_sekvensnummer");
        }

        LOG.info("Oppdaterer DB med jobId={}, fraSekvensnummer={}, tilSekvensnummer={}",
                jobId, fraSekvensnummer, tilSekvensnummer
        );

        service.createNew(jobId, fraSekvensnummer, tilSekvensnummer);
    }

    @Override
    public void afterChunkError(ChunkContext context) {
    }
}