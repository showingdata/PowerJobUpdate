package tech.powerjob.samples;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 主类
 *
 * @author tjq
 * @since 2020/4/17
 */
@Slf4j
@EnableScheduling
@SpringBootApplication
public class SampleApplication {
    public static void main(String[] args) {
        SpringApplication.run(SampleApplication.class, args);
        log.info("启动完成");

    }
}
