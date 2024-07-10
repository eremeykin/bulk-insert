package pete.eremeykin.bulkinsert.input.generator;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputFileItem;

import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(InputGeneratorProperties.class)
class InputGeneratorJobConfiguration {
    private final static String JOB_NAME = "inputFileGenerationJob";
    private final static String GENERATE_INPUT_FILE_STEP_NAME = "generateInputFile";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final InputGeneratorProperties properties;

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
                .reader(generatingRandomItemReader())
                .writer(dummyItemWriter())
                .build();
    }

    @Bean
    ItemReader<InputFileItem> generatingRandomItemReader() {
        AtomicInteger itemsProduced = new AtomicInteger();
        return () -> {
            if (itemsProduced.incrementAndGet() > 10) return null;
            return new InputFileItem(
                    "Test name",
                    "Test artist",
                    "Test album name"
            );
        };
    }

    @Bean
    ItemWriter<InputFileItem> dummyItemWriter() {
        return chunk -> {
            for (InputFileItem item : chunk.getItems()) {
                System.out.println(item);
            }
        };
    }
}

