package ceo.peak.data.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class TestController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    @PutMapping("/v1/saramin")
    public String testJob() throws Exception {
        String value = LocalDateTime.now().toString();

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", value)
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        log.info("Starting job with parameters: {}", jobParameters);

        JobExecution jobExecution = jobLauncher.run(jobRegistry.getJob("saraminJob"), jobParameters);

        log.info("Job finished with status: {}", jobExecution.getStatus());

        return "Job status: " + jobExecution.getStatus();
    }

}
