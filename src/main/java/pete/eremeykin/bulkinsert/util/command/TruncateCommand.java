package pete.eremeykin.bulkinsert.util.command;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;

@Slf4j
@ShellComponent
@RequiredArgsConstructor
@Component
class TruncateCommand {
    private final JdbcTemplate jdbcTemplate;

    @ShellMethod(value = "Truncate test table", key = {"truncate", "tr"})
    public String countRows() {
        int rowsDeleted = jdbcTemplate.update("DELETE FROM %s".formatted(SchemaInfo.TEST_TABLE_NAME));
        return "%,d rows have been deleted from '%s' table".formatted(rowsDeleted, SchemaInfo.TEST_TABLE_NAME);
    }
}
