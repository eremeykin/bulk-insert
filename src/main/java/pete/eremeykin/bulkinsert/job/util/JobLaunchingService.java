package pete.eremeykin.bulkinsert.job.util;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JobParametersConverter;


@RequiredArgsConstructor
public class JobLaunchingService<T> {
    private final Job jobToLaunch;
    private final JobLauncher jobLauncher;
    private final JobParametersConverter<T> jobParametersConverter;

    public JobExecution launchJob(T jobParameters) throws Exception {
        JobParameters parameters = jobParametersConverter.parametersFromObject(jobParameters);
        return jobLauncher.run(jobToLaunch, parameters);
    }
}
