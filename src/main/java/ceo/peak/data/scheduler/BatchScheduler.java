package ceo.peak.data.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class BatchScheduler {

    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;
    private final JobRepository jobRepository;
    private final Job saraminJob; // Job 객체 주입

    @Scheduled(cron = "0 0 4 * * 6") // 매주 토요일 새벽 4시에 실행
    public String runSaraminJob() throws Exception {
        String value = LocalDateTime.now().toString();

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("date", value)
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        log.info("Starting job with parameters: {}", jobParameters);

        JobExecution jobExecution = jobLauncher.run(saraminJob, jobParameters);

        log.info("Job finished with status: {}", jobExecution.getStatus());

        return "Job status: " + jobExecution.getStatus();
    }

    @Scheduled(cron = "0 0 2 * * *") // 매일 새벽 2시에 실행
    public void restartSaraminJob() {
        try {
            String jobName = "saraminJob";
            LocalDateTime cutoffTime = LocalDateTime.now().minusHours(24);

            // 1. JobInstance 조회 (최근 100개)
            List<JobInstance> jobInstances = jobExplorer.findJobInstancesByJobName(jobName, 0, 100);

            // 2. 모든 Execution 추출 + 필터링
            List<JobExecution> failedExecutions = jobInstances.stream()
                    .flatMap(instance -> jobExplorer.getJobExecutions(instance).stream())
                    .filter(exec -> exec.getStatus() == BatchStatus.FAILED)
                    .filter(exec -> exec.getStartTime() != null
                            && exec.getStartTime().isAfter(cutoffTime))
                    .sorted((e1, e2) -> Objects.requireNonNull(e2.getStartTime()).compareTo(e1.getStartTime()))
                    .toList();

            // 3. 재시작 로직 (이하 동일)
            if (!failedExecutions.isEmpty()) {
                JobExecution latestFailed = failedExecutions.get(0);
                log.info("Restarting failed job: {}", latestFailed.getId());

                JobParameters newJobParameters = new JobParametersBuilder(latestFailed.getJobParameters())
                        .addLong("time", System.currentTimeMillis())
                        .toJobParameters();

                jobLauncher.run(saraminJob, newJobParameters);
                return;
            }

            // 4. 새 작업 실행
            JobParameters params = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(saraminJob, params);

        } catch (Exception e) {
            log.error("Job execution failed", e);
        }
    }

}
