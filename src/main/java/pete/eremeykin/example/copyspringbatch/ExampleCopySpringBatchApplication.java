package pete.eremeykin.example.copyspringbatch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.shell.boot.ShellRunnerAutoConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import pete.eremeykin.bulkinsert.input.InputItem;
import pete.eremeykin.bulkinsert.input.OutputItem;

import javax.sql.DataSource;
import java.util.Map;
import java.util.UUID;

@SpringBootApplication(scanBasePackages = "pete.eremeykin.example.copyspringbatch",
        exclude = {ShellRunnerAutoConfiguration.class})
class ExampleCopySpringBatchApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ExampleCopySpringBatchApplication.class);
        Map<String, Object> properties = Map.of(
                "spring.config.name", "none",
                "spring.datasource.url", "jdbc:postgresql://localhost:7432/postgres?currentSchema=public&reWriteBatchedInserts=true",
                "spring.datasource.username", "user",
                "spring.datasource.password", "password",
                "spring.batch.job.enabled", "true", // job is executed on application startup
                "spring.batch.jdbc.initialize-schema", "always"
        );
        app.setDefaultProperties(properties);
        app.run("id=" + UUID.randomUUID());
    }

    @Configuration
    @RequiredArgsConstructor
    static class BathConfiguration {
        private final JobRepository jobRepository;
        private final PlatformTransactionManager platformTransactionManager;
        private final DataSource dataSource;

        @Bean
        Job batchLoadJob() {
            return new JobBuilder("loadingJobName", jobRepository)
                    .start(batchLoadStep())
                    .build();
        }

        @Bean
        AbstractStep batchLoadStep() {
            return new StepBuilder("loadingStepName", jobRepository)
                    .<InputItem, OutputItem>chunk(1_000, platformTransactionManager)
                    .reader(itemReader())
                    .processor(itemProcessor())
                    .writer(itemWriter())
                    .build();
        }

        @Bean
        ItemReader<InputItem> itemReader() {
            FlatFileItemReader<InputItem> flatFileItemReader = new FlatFileItemReader<>();
            flatFileItemReader.setResource(new FileSystemResource("test.csv"));
            DefaultLineMapper<InputItem> lineMapper = new DefaultLineMapper<>();
            DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
            tokenizer.setNames(
                    "name",
                    "artist",
                    "albumName"
            );
            lineMapper.setLineTokenizer(tokenizer);
            BeanWrapperFieldSetMapper<InputItem> objectBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
            objectBeanWrapperFieldSetMapper.setTargetType(InputItem.class);
            lineMapper.setFieldSetMapper(objectBeanWrapperFieldSetMapper);
            flatFileItemReader.setLineMapper(lineMapper);
            return flatFileItemReader;
        }

        @Bean
        ItemWriter<OutputItem> itemWriter() {
            return new CopyItemWriter(dataSource);
        }

        @Bean
        ItemProcessor<InputItem, OutputItem> itemProcessor() {
            return item -> new OutputItem(
                    UUID.randomUUID(),
                    item.getName(),
                    item.getArtist(),
                    item.getAlbumName()
            );
        }
    }
}


