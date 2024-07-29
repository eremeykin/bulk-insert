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
class CountRowsCommand {
    private final JdbcTemplate jdbcTemplate;

    @ShellMethod(value = "Print number of rows currently present in the test table", key = {"count-rows", "cr"})
    public String countRows() {
        StringBuilder result = new StringBuilder();
        for (SchemaInfo.TestTable testTable : SchemaInfo.TestTable.values()) {
            String tableName = testTable.getTableName();
            Long rows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM %s".formatted(tableName), Long.class);
            result.append("'%s' table contains %,d rows.\n".formatted(tableName, rows));
        }
        return result.toString();
    }
}
