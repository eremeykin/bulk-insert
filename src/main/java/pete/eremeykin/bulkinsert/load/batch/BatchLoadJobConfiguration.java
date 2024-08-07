package pete.eremeykin.bulkinsert.load.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputItem;
import pete.eremeykin.bulkinsert.input.OutputItem;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JacksonJobParametersConverter;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JobParametersConverter;

import java.util.Map;
import java.util.UUID;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(BatchLoadProperties.class)
class BatchLoadJobConfiguration {
    private static final String JOB_NAME = "batchLoadJob";
    private static final String LOAD_SOURCE_FILE_STEP_NAME = "loadSourceFile";
    private static final String MANAGER_SUFFIX = ".manager";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final BatchLoadJobParameters jobParameters;

    @Bean
    Job batchLoadJob(Step loadStepManager) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(loadStepManager)
                .build();
    }

    @Bean
    @ConfigurationProperties("bulkinsert.executor")
    ThreadPoolTaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor();
    }

    @Bean
    @WriterType.CopyAsyncWriterQualifier
    AsyncTaskExecutor writerTaskExecutor() {
        ThreadPoolTaskExecutor writerExecutor = new ThreadPoolTaskExecutor();
        BeanUtils.copyProperties(taskExecutor(), writerExecutor);
        writerExecutor.setThreadNamePrefix("WriterExec-");
        return writerExecutor;
    }

    @Bean
    @JobScope
    Step loadStepManager(
            @BatchLoadQualifier Step batchLoadStep,
            @TaskletLoadQualifier Step taskletLoadStep,
            @Value("#{jobParameters['writerType']}") WriterType writerType
    ) {
        Step loadStep = writerType == WriterType.COPY_NON_BATCH ? taskletLoadStep : batchLoadStep;
        ThreadPoolTaskExecutor taskExecutor = taskExecutor();
        return new StepBuilder(LOAD_SOURCE_FILE_STEP_NAME + MANAGER_SUFFIX, jobRepository)
                .partitioner(LOAD_SOURCE_FILE_STEP_NAME, partitioner())
                .step(loadStep)
                .gridSize(taskExecutor.getMaxPoolSize())
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    @TaskletLoadQualifier
    AbstractStep taskletLoadStep(@TaskletLoadQualifier Tasklet copyTasklet) {
        return new StepBuilder(LOAD_SOURCE_FILE_STEP_NAME, jobRepository)
                .tasklet(copyTasklet, platformTransactionManager)
                .build();
    }

    @Bean
    @BatchLoadQualifier
    AbstractStep batchLoadStep(
            @BatchLoadQualifier ItemReader<InputItem> itemReader,
            BatchLoadProperties batchLoadProperties,
            ItemWriter<OutputItem> itemWriter
    ) {
        return new StepBuilder(LOAD_SOURCE_FILE_STEP_NAME, jobRepository)
                .<InputItem, OutputItem>chunk(batchLoadProperties.getChunkSize(), platformTransactionManager)
                .reader(itemReader)
                .processor(this::createOutputItem)
                .writer(itemWriter)
                .build();
    }

    private OutputItem createOutputItem(InputItem inputItem) {
        return new OutputItem(
                UUID.randomUUID(),
                inputItem.getName(),
                inputItem.getArtist(),
                inputItem.getAlbumName()
        );
    }

    @StepScope
    @Bean
    @BatchLoadQualifier
    ItemStreamWriter<OutputItem> itemWriter(
            @WriterType.CopyAsyncWriterQualifier ItemStreamWriter<OutputItem> copyAsyncWriter,
            @WriterType.CopySyncWriterQualifier ItemStreamWriter<OutputItem> copySyncWriter,
            @WriterType.InsertsWriterQualifier ItemStreamWriter<OutputItem> insertsItemWriter,
            BatchLoadJobParameters jobParameters
    ) {
        return switch (jobParameters.getWriterType()) {
            case COPY_ASYNC -> copyAsyncWriter;
            case COPY_SYNC -> copySyncWriter;
            case INSERTS_DEFAULT, INSERTS_ADVANCED -> insertsItemWriter;
            case COPY_NON_BATCH ->
                    throw new IllegalArgumentException("itemWriter can't be used with " + jobParameters.getWriterType());
        };
    }

    @Bean
    Partitioner partitioner() {
        return new LineCountingPartitioner(lineCountingService());
    }

    @Bean
    LineCountingService lineCountingService() {
        return new LineCountingService(jobParameters);
    }

    @Bean(name = "batchLoadJobLaunchingService")
    JobLaunchingService<BatchLoadJobParameters> jobLaunchingService(JobLauncher jobLauncher, Job batchLoadJob) {
        return new JobLaunchingService<>(
                batchLoadJob,
                jobLauncher,
                jobParametersConverter()
        );
    }

    @Bean
    JobParametersConverter<BatchLoadJobParameters> jobParametersConverter() {
        return new JacksonJobParametersConverter<>(new ObjectMapper(), BatchLoadJobParameters.class);
    }

    @StepScope
    @Bean
    BatchLoadJobParameters batchLoadJobParameters(
            @Value("#{jobParameters}") Map<String, Object> jobParameters,
            JobParametersConverter<BatchLoadJobParameters> jobParametersConverter) {
        return jobParametersConverter.objectFromMap(jobParameters);
    }
}
