package com.benturney.batch.config;

import com.benturney.batch.FileMessageToJobRequest;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileReadingMessageSource.WatchEventType;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;

import java.io.File;

@Configuration
public class IntegrationConfig {

    private final Job testJob;
    private final JobLauncher jobLauncher;

    public IntegrationConfig(Job testJob, JobLauncher jobLauncher) {
        this.testJob = testJob;
        this.jobLauncher = jobLauncher;
    }

    @Bean
    IntegrationFlow processFiles(@Value("${input-directory:${HOME}/Desktop/in}") File in) {
        return IntegrationFlows
                .from(Files.inboundAdapter(in)
                                .autoCreateDirectory(true)
                                .preventDuplicates(true)
                                .ignoreHidden(true)
                                .useWatchService(true)
                                .watchEvents(WatchEventType.CREATE, WatchEventType.MODIFY)
                                .filter(new SimplePatternFileListFilter("*.csv")),
                        config -> config.poller(Pollers.fixedRate(1000).maxMessagesPerPoll(1)))
                .transform(fileMessageToJobRequest())
                .handle(jobLaunchingGateway())
                .log()
                .get();
    }

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest() {
        return new FileMessageToJobRequest(testJob, "input.file.name");
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway() {
        return new JobLaunchingGateway(jobLauncher);
    }
}
