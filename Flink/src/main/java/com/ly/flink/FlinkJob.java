package com.ly.flink;

import java.util.Properties;

/**
 * @author wangly
 */
public interface FlinkJob {
    /**
     * flink job 运行
     *
     * @param properties 运行参数
     */
    void run(Properties properties);
}
