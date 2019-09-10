package com.tianlangstudio.data.datax.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhuhq on 2016/4/29.
 */
public class ConfigTest {
    @Test
    public void testWithFallback() {
        Config config = ConfigFactory.load("master.conf");
        config = config.withFallback(ConfigFactory.load());
        //config = config.resolveWith(ConfigFactory.load("master.conf"));

        String testKeyInAll = config.getString("testKeyInAll");
        Assert.assertEquals("master", testKeyInAll);

        String testKeyInMaster = config.getString("testKeyInMaster");
        Assert.assertEquals("master", testKeyInMaster);

        String testKeyInApplication = config.getString("testKeyInApplication");
        Assert.assertEquals("application", testKeyInApplication);
    }

}
