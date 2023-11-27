package com.ly.flink.jobs;

import com.ly.flink.FlinkJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wangly
 */
@Slf4j
public class TestJob implements FlinkJob {
    @Override
    public void run(Properties properties) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> personDataStreamSource = env.addSource(new RichParallelSourceFunction<>() {
            private boolean flag = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void run(SourceContext<Person> sourceContext) throws InterruptedException {
                while (flag) {
                    List<Person> personList = new ArrayList<>();
                    personList.add(new Person("Fred", 35));
                    personList.add(new Person("Wilma", 35));
                    personList.add(new Person("Pebbles", 2));
                    personList.forEach(person -> log.info("person: {}", person));
                    personList.forEach(sourceContext::collect);
                    TimeUnit.MILLISECONDS.sleep(10000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        DataStream<Person> adults = personDataStreamSource.filter((FilterFunction<Person>) person -> person.age >= 18);

        log.info("hello world aaa");
        adults.print();

        try {
            env.execute();
        } catch (Exception e) {
            log.error("-{}-作业运行异常", TestJob.class, e);
            throw new RuntimeException(e);
        }
    }

    public static class Person {
        public String name;
        public Integer age;


        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return this.name + ": age " + this.age.toString();
        }
    }
}
