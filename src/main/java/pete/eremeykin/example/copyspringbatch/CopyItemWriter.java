package pete.eremeykin.example.copyspringbatch;

import lombok.RequiredArgsConstructor;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import pete.eremeykin.bulkinsert.input.OutputItem;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

@RequiredArgsConstructor
class CopyItemWriter implements ItemStreamWriter<OutputItem> {
    private final DataSource dataSource;
    private OutputStream pgOutputStream;
    private Connection connection;

    @Override
    public void write(Chunk<? extends OutputItem> chunk) throws Exception {
        for (OutputItem outputItem : chunk) {
            String copyLine = String.join(",",
                    outputItem.getId().toString(),
                    outputItem.getName(),
                    outputItem.getArtist(),
                    outputItem.getAlbumName()
            );
            pgOutputStream.write(copyLine.getBytes(StandardCharsets.UTF_8));
            pgOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            connection = new DataSourceUtilsConnection(dataSource);
            pgOutputStream = new PGCopyOutputStream(connection.unwrap(PGConnection.class), """
                    COPY songs (id, name, artist, album_name)
                    FROM STDIN
                    DELIMITER ','
                    CSV;
                    """);
        } catch (SQLException e) {
            throw new ItemStreamException("Unable to open Postgres copy stream", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            try {
                pgOutputStream.close();
            } finally {
                connection.close();
            }
        } catch (SQLException | IOException exception) {
            throw new ItemStreamException("Unable to close writer", exception);
        }
    }
}

