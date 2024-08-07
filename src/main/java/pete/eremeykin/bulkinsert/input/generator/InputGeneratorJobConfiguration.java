package pete.eremeykin.bulkinsert.input.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputItem;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;
import pete.eremeykin.bulkinsert.job.util.StatusReportingChunkListener;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JacksonJobParametersConverter;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JobParametersConverter;

import java.util.Map;

@Configuration
@EnableConfigurationProperties(InputGeneratorProperties.class)
class InputGeneratorJobConfiguration {
    private final static String JOB_NAME = "inputFileGenerationJob";
    private final static String GENERATE_INPUT_FILE_STEP_NAME = "generateInputFile";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final InputGeneratorProperties properties;
    private final ItemReader<InputItem> itemReader;
    private final ItemWriter<InputItem> itemWriter;

    InputGeneratorJobConfiguration(JobRepository jobRepository,
                                   PlatformTransactionManager platformTransactionManager,
                                   InputGeneratorProperties properties,
                                   @InputGeneratorQualifier
                                   ItemReader<InputItem> itemReader,
                                   @InputGeneratorQualifier
                                   ItemWriter<InputItem> itemWriter) {
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
        this.properties = properties;
        this.itemReader = itemReader;
        this.itemWriter = itemWriter;
    }

    @Bean
    Job inputFileGenerationJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(generateInputFile())
                .build();
    }

    @Bean
    Step generateInputFile() {
        return new StepBuilder(GENERATE_INPUT_FILE_STEP_NAME, jobRepository)
                .<InputItem, InputItem>chunk(properties.getChunkSize(), platformTransactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .listener(new StatusReportingChunkListener())
                .build();
    }

    @Bean(name = "inputFileGeneratorJobParametersConverter")
    JobParametersConverter<InputGeneratorJobParameters> jobParametersConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        return new JacksonJobParametersConverter<>(objectMapper, InputGeneratorJobParameters.class);
    }

    @Bean(name = "inputFileGeneratorJobLaunchingService")
    JobLaunchingService<InputGeneratorJobParameters> jobLaunchingService(JobLauncher jobLauncher) {
        return new JobLaunchingService<>(
                inputFileGenerationJob(),
                jobLauncher,
                jobParametersConverter()
        );
    }

    @JobScope
    @Bean(name = "inputFileGeneratorJobParameters")
    InputGeneratorJobParameters jobParameters(
            @Value("#{jobParameters}") Map<String, Object> jobParameters,
            JobParametersConverter<InputGeneratorJobParameters> jobParametersConverter) {
        return jobParametersConverter.objectFromMap(jobParameters);
    }
}

