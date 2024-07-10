package pete.eremeykin.bulkinsert.input.generator;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "bulkinsert.input.generator")
class InputGeneratorProperties {
    private int chunkSize;
}
