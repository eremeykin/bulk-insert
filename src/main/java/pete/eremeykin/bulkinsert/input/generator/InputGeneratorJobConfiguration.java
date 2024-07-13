package pete.eremeykin.bulkinsert.input.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.job.parameters.converter.JacksonJobParametersConverter;
import pete.eremeykin.bulkinsert.job.parameters.converter.JobParametersConverter;
import pete.eremeykin.bulkinsert.job.status.StatusReportingChunkListener;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(InputGeneratorProperties.class)
class InputGeneratorJobConfiguration {
    private final static String JOB_NAME = "inputFileGenerationJob";
    private final static String GENERATE_INPUT_FILE_STEP_NAME = "generateInputFile";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final InputGeneratorProperties properties;
    private final ItemReader<InputFileItem> itemReader;
    private final ItemWriter<InputFileItem> itemWriter;

    @Bean
    Job inputFileGenerationJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(generateInputFile())
                .build();
    }

    @Bean
    Step generateInputFile() {
        return new StepBuilder(GENERATE_INPUT_FILE_STEP_NAME, jobRepository)
                .<InputFileItem, InputFileItem>chunk(properties.getChunkSize(), platformTransactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .listener(new StatusReportingChunkListener())
                .build();
    }

    @Bean
    JobParametersConverter<InputGeneratorJobParameters> jobParametersConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        return new JacksonJobParametersConverter<>(objectMapper, InputGeneratorJobParameters.class);
    }

    @JobScope
    @Bean
    InputGeneratorJobParameters inputGeneratorJobParameters(
            @Value("#{jobParameters}") Map<String, Object> jobParameters,
            JobParametersConverter<InputGeneratorJobParameters> jobParametersConverter) {
        return jobParametersConverter.objectFromMap(jobParameters);
    }
}

