package pete.eremeykin.bulkinsert.job.util;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import pete.eremeykin.bulkinsert.job.util.parameters.converter.JobParametersConverter;

import java.time.Duration;
import java.time.LocalDateTime;


@RequiredArgsConstructor
public class JobLaunchingService<T> {
    private final Job jobToLaunch;
    private final JobLauncher jobLauncher;
    private final JobParametersConverter<T> jobParametersConverter;

    public String launchJob(T jobParameters) throws Exception {
        JobParameters parameters = jobParametersConverter.parametersFromObject(jobParameters);
        JobExecution jobExecution = jobLauncher.run(jobToLaunch, parameters);
        LocalDateTime endTime = jobExecution.getEndTime();
        LocalDateTime startTime = jobExecution.getStartTime();
        String duration = "";
        if (endTime != null && startTime != null) {
            duration = " in %.2f seconds".formatted(Duration.between(startTime, endTime).toMillis() / 1000.0);
        }
        return "Job completed: " + jobParameters + duration;
    }
}
