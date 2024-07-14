package pete.eremeykin.bulkinsert.job.util.parameters.converter;

import org.springframework.batch.core.JobParameters;

import java.util.Map;

public interface JobParametersConverter<T> {
    JobParameters parametersFromObject(T inputGeneratorJobParameters);

    T objectFromMap(Map<String, Object> jobParameters);

}
