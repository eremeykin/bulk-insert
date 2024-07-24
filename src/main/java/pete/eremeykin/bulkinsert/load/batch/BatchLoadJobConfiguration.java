package pete.eremeykin.bulkinsert.load.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;
import pete.eremeykin.bulkinsert.job.util.StatusReportingChunkListener;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JacksonJobParametersConverter;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JobParametersConverter;

import java.util.Map;

@Configuration
class BatchLoadJobConfiguration {
    private static final String JOB_NAME = "batchLoadJob";
    private static final String LOAD_SOURCE_FILE_STEP_NAME = "loadSourceFile";
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    @BatchLoadQualifier
    private final ItemReader<InputFileItem> itemReader;
    @BatchLoadQualifier
    private final ItemWriter<InputFileItem> itemWriter;
    private final BatchLoadJobParameters jobParameters;

    BatchLoadJobConfiguration(JobRepository jobRepository,
                              PlatformTransactionManager platformTransactionManager,
                              @BatchLoadQualifier
                              ItemReader<InputFileItem> itemReader,
                              @BatchLoadQualifier
                              ItemWriter<InputFileItem> itemWriter,
                              @Qualifier("batchLoadJobParameters")
                              BatchLoadJobParameters jobParameters) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.itemReader = itemReader;
        this.itemWriter = itemWriter;
        this.jobParameters = jobParameters;
    }

    @Bean
    Job batchLoadJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(loadStep())
                .build();
    }

    @Bean
    @ConfigurationProperties("bulkinsert.executor")
    TaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor();
    }

    @JobScope
    @Bean
    Step loadStep() {
        return new StepBuilder(LOAD_SOURCE_FILE_STEP_NAME, jobRepository)
                .<InputFileItem, InputFileItem>chunk(jobParameters.getChunkSize(), platformTransactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .listener(new StatusReportingChunkListener())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean(name = "batchLoadJobLaunchingService")
    JobLaunchingService<BatchLoadJobParameters> jobLaunchingService(JobLauncher jobLauncher) {
        return new JobLaunchingService<>(
                batchLoadJob(),
                jobLauncher,
                jobParametersConverter()
        );
    }

    @Bean(name = "batchLoadJobParametersConverter")
    JobParametersConverter<BatchLoadJobParameters> jobParametersConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        return new JacksonJobParametersConverter<>(objectMapper, BatchLoadJobParameters.class);
    }

    @JobScope
    @Bean(name = "batchLoadJobParameters")
    BatchLoadJobParameters jobParameters(
            @Value("#{jobParameters}") Map<String, Object> jobParameters,
            JobParametersConverter<BatchLoadJobParameters> jobParametersConverter) {
        return jobParametersConverter.objectFromMap(jobParameters);
    }

    @StepScope
    @Bean(name = "batchLoadJobParametersAtSteScope")
    BatchLoadJobParameters jobParametersAtStepScope(
            @Value("#{jobParameters}") Map<String, Object> jobParameters,
            JobParametersConverter<BatchLoadJobParameters> jobParametersConverter) {
        return jobParametersConverter.objectFromMap(jobParameters);
    }
}
