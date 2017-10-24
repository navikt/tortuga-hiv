package no.nav.opptjening.hiv.hendelser;

import no.nav.opptjening.hiv.hendelser.batch.HendelseItemProcessor;
import no.nav.opptjening.hiv.hendelser.batch.HendelseItemReader;
import no.nav.opptjening.hiv.hendelser.batch.HendelseItemWriter;
import no.nav.opptjening.hiv.hendelser.batch.ItemChunkListener;
import no.nav.opptjening.hiv.hendelser.support.BatchLoggService;
import no.nav.opptjening.skatt.dto.HendelseDto;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    @Value("${hiv.chunk-size:10}")
    private int chunkSize;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public ChunkListener chunkListener(BatchLoggService service) {
        return new ItemChunkListener(service);
    }

    @Bean
    public Step step1(HendelseItemReader itemReader, HendelseItemWriter itemWriter, ChunkListener chunkListener) {
        SimpleStepBuilder<HendelseDto, InntektKafkaHendelseDto> step = stepBuilderFactory.get("step1")
                .chunk(chunkSize);

        step.reader(itemReader);
        step.processor(new HendelseItemProcessor());
        step.writer(itemWriter);
        step.listener(chunkListener);

        return step.build();
    }

    @Bean
    public Job job(@Qualifier("step1") Step step1) throws Exception {
        return jobBuilderFactory.get("hentInntektsVarslinger")
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .build();
    }
}
