package pete.eremeykin.bulkinsert.load.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JacksonJobParametersConverter;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JobParametersConverter;

import java.util.Map;

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
        ThreadPoolTaskExecutor partitionedStepExecutor = taskExecutor();
        ThreadPoolTaskExecutor writerExecutor = new ThreadPoolTaskExecutor();
        writerExecutor.setCorePoolSize(partitionedStepExecutor.getCorePoolSize());
        writerExecutor.setMaxPoolSize(partitionedStepExecutor.getMaxPoolSize());
        writerExecutor.setThreadNamePrefix("WriterExec-");
        return writerExecutor;
    }

    @Bean
    Step loadStepManager(Step loadStep) {
        ThreadPoolTaskExecutor taskExecutor = taskExecutor();
        return new StepBuilder(LOAD_SOURCE_FILE_STEP_NAME + MANAGER_SUFFIX, jobRepository)
                .partitioner(LOAD_SOURCE_FILE_STEP_NAME, partitioner())
                .step(loadStep)
                .gridSize(taskExecutor.getMaxPoolSize())
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    Step loadStep(
            @BatchLoadQualifier ItemReader<InputFileItem> itemReader,
            BatchLoadProperties batchLoadProperties,
            ItemWriter<InputFileItem> itemWriter
    ) {
        return new StepBuilder(LOAD_SOURCE_FILE_STEP_NAME, jobRepository)
                .<InputFileItem, InputFileItem>chunk(batchLoadProperties.getChunkSize(), platformTransactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .build();
    }

    @StepScope
    @Bean
    ItemStreamWriter<InputFileItem> itemWriter(
            @WriterType.CopyAsyncWriterQualifier ItemWriter<InputFileItem> copyAsyncWriter,
            @WriterType.CopySyncWriterQualifier ItemWriter<InputFileItem> copySyncWriter,
            @WriterType.InsertsWriterQualifier ItemWriter<InputFileItem> insertsItemWriter,
            @Value("#{jobParameters['writerType']}") WriterType writerType
    ) {
        ItemWriter<InputFileItem> delegate;
        switch (writerType) {
            case COPY_ASYNC -> delegate = copyAsyncWriter;
            case COPY_SYNC -> delegate = copySyncWriter;
            case INSERTS -> delegate = insertsItemWriter;
            default -> throw new IllegalStateException("Unknown writer type: " + writerType);
        }
        return new ItemStreamWriter.DelegateItemStreamWriter<>(delegate);
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
