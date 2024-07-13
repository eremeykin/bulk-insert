package pete.eremeykin.bulkinsert.job.parameters.converter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.util.Map;

@RequiredArgsConstructor
public class JacksonJobParametersConverter<T> implements JobParametersConverter<T> {
    private final ObjectMapper objectMapper;
    private final Class<T> parametersClass;

    public JobParameters parametersFromObject(T inputGeneratorJobParameters) {
        Map<String, Object> map = objectMapper.convertValue(inputGeneratorJobParameters, new TypeReference<>() {
        });
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        map.forEach((key, value) ->
                jobParametersBuilder.addJobParameter(key, value, (Class<Object>) value.getClass()));
        return jobParametersBuilder.toJobParameters();
    }

    public T objectFromMap(Map<String, Object> jobParameters) {
        return objectMapper.convertValue(jobParameters, parametersClass);
    }
}
