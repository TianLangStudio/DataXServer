package esun.fbi.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.Test;

/**
 * Created by zhuhq on 2016/4/29.
 */
public class ConfigTest {
    public static void main(String args[]) {
        Config config = ConfigFactory.load("master.conf");
        config = config.withFallback(ConfigFactory.load());
        //config = config.resolveWith(ConfigFactory.load("master.conf"));
        System.out.println(config.getString("datax.master.host"));
        System.out.println(config.getString("hello"));
    }

}
