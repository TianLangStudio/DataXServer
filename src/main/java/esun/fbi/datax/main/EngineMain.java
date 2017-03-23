package esun.fbi.datax.main;

import com.taobao.datax.common.constants.ExitStatus;
import com.taobao.datax.engine.conf.EngineConf;
import com.taobao.datax.engine.conf.JobConf;
import com.taobao.datax.engine.conf.ParseXMLUtil;
import com.taobao.datax.engine.conf.PluginConf;
import com.taobao.datax.engine.schedule.Engine;
import com.taobao.datax.engine.tools.JobConfGenDriver;

import java.util.Map;

/**
 * Created by zhuhq on 2015/11/20.
 */
public class EngineMain {

    public static  void main(String args[]) throws Exception{
        String jobDescFile = null;
        if (args.length < 1) {
            System.exit(JobConfGenDriver.produceXmlConf());
        } else if (args.length == 1) {
            jobDescFile = args[0];
        } else {
            System.out.printf("Usage: java -jar engine.jar job.xml .");
            System.exit(ExitStatus.FAILED.value());
        }

        Engine.confLog("BEFORE_CHRIST");
        JobConf jobConf = ParseXMLUtil.loadJobConfig(jobDescFile);
        Engine.confLog(jobConf.getId());
        EngineConf engineConf = ParseXMLUtil.loadEngineConfig();
        Map<String, PluginConf> pluginConfs = ParseXMLUtil.loadPluginConfig();

        Engine engine = new Engine(engineConf, pluginConfs);

        int retcode = 0;
        try {
            retcode = engine.start(jobConf);
        } catch (Exception e) {
            e.printStackTrace();
            //logger.error(ExceptionTracker.trace(e));
            //System.exit(ExitStatus.FAILED.value());
        }
        System.exit(retcode);
    }
}
