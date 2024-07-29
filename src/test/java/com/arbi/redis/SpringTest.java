package com.arbi.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;
import static org.mockito.Mockito.*;

@SpringBootTest
public class SpringTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void TestRedisTemplate() {
        assertNotNull(redisTemplate);
    }
}
