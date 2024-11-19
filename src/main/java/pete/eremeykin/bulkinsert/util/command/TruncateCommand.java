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

    @ShellMethod(value = "Truncate test tables", key = {"truncate", "tr"})
    public String countRows() {
        StringBuilder result = new StringBuilder();
        for (SchemaInfo.TestTable testTable : SchemaInfo.TestTable.values()) {
            String tableName = testTable.getTableName();
            jdbcTemplate.update("TRUNCATE TABLE %s".formatted(tableName));
            result.append("'%s' table has been truncated\n".formatted(tableName));
        }
        return result.toString();
    }
}
