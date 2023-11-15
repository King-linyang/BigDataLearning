package com.ly.flink.jobs;

import com.ly.flink.FlinkJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author wangly
 */
@Slf4j
public class TestJob implements FlinkJob {
    @Override
    public void run(Properties properties) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        DataStream<Person> adults = flintstones.filter((FilterFunction<Person>) person -> person.age >= 18);

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
