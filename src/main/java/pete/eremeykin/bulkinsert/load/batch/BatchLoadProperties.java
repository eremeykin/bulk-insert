package pete.eremeykin.bulkinsert.load.batch;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "bulkinsert.batch.load")
class BatchLoadProperties {
    private int chunkSize;
}
