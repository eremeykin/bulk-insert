package pete.eremeykin.bulkinsert.diagnostics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@ShellComponent
@RequiredArgsConstructor
@Component
class DatabaseInfoCommand {
    private final Flyway flyway;

    @ShellMethod(value = "Print current state of the database schema")
    public String dbInfo() {
        MigrationInfo currentMigrationInfo = flyway.info().current();
        return String.join("\n",
                List.of(
                        "Version: " + currentMigrationInfo.getVersion(),
                        "Script: " + currentMigrationInfo.getScript()
                )
        );
    }
}
