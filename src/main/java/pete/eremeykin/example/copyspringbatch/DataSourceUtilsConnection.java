package pete.eremeykin.example.copyspringbatch;

import lombok.experimental.Delegate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;

class DataSourceUtilsConnection implements Connection {
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
