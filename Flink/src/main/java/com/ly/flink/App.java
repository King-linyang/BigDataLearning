package com.ly.flink;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Hello world!
 *
 * @author wangly
 */
@Slf4j
public class App {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("jobClass", "TestJob");
        String jobClass = properties.getProperty("jobClass");

        FlinkJob jobRunner = findJobByClass(jobClass);
        assert jobRunner != null;
        jobRunner.run(properties);
        System.out.println("Hello World!");
    }

    public static FlinkJob findJobByClass(String jobName) {
        ServiceLoader<FlinkJob> loader = ServiceLoader.load(FlinkJob.class);
        for (FlinkJob jobRunner : loader) {
            String simpleName = jobRunner.getClass().getSimpleName();
            if (simpleName.equalsIgnoreCase(jobName.toLowerCase())) {
                return jobRunner;
            }
        }
        log.error("find no job for name : {}", jobName);
        return null;
    }
}
