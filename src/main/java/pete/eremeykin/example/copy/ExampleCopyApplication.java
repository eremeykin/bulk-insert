package pete.eremeykin.example.copy;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.shell.boot.ShellRunnerAutoConfiguration;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;
import java.util.UUID;

@RequiredArgsConstructor
@SpringBootApplication(scanBasePackages = "pete.eremeykin.example.copy",
        exclude = {ShellRunnerAutoConfiguration.class})
class ExampleCopyApplication implements CommandLineRunner {
    private final DataSource dataSource;

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ExampleCopyApplication.class);
        Map<String, Object> properties = Map.of(
                "spring.config.name", "none",
                "spring.datasource.url", "jdbc:postgresql://localhost:7432/postgres?currentSchema=public",
                "spring.datasource.username", "user",
                "spring.datasource.password", "password",
                "spring.batch.job.enabled", "false"
        );
        app.setDefaultProperties(properties);
        app.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        try (var connection = new DataSourceUtilsConnection(dataSource);
             var pgCopyOutputStream = new PGCopyOutputStream(connection.unwrap(PGConnection.class), """
                     COPY songs (id, name, artist, album_name)
                     FROM STDIN
                     DELIMITER ','
                     CSV;
                     """);
             var fileInputStream = Files.newInputStream(Path.of("test.csv"));
             var fileReader = new BufferedReader(new InputStreamReader(fileInputStream));
        ) {
            String line;
            while ((line = fileReader.readLine()) != null) {
                String copyLine = UUID.randomUUID() + "," + line + "\n";
                pgCopyOutputStream.write(copyLine.getBytes(StandardCharsets.UTF_8));
            }
        }
    }


    private static class DataSourceUtilsConnection implements Connection {
        private final DataSource dataSource;
        @Delegate
        private final Connection delegate;

        public DataSourceUtilsConnection(DataSource dataSource) {
            this.dataSource = dataSource;
            this.delegate = DataSourceUtils.getConnection(dataSource);
        }

        @Override
        public void close() {
            DataSourceUtils.releaseConnection(delegate, dataSource);
        }
    }
}
