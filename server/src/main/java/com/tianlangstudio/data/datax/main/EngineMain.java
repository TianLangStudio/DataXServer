package com.tianlangstudio.data.datax.main;


import com.tianlangstudio.data.datax.core.Engine;

/**
 * Created by zhuhq on 2015/11/20.
 */
public class EngineMain {

    public static  void main(String args[]) throws Exception{
        String jobDescFile = null;
        if (args.length < 1) {
            System.exit(0);
        } else if (args.length == 1) {
            jobDescFile = args[0];
        } else {
            System.out.printf("Usage: java -jar engine.jar job.xml .");
            System.exit(-1);
        }
        Engine engine = new Engine();

        int returnCode = 0;
        try {
            engine.start(jobDescFile, "-1");
        } catch (Exception e) {
            returnCode = -1;
            e.printStackTrace();
            //logger.error(ExceptionTracker.trace(e));
            //System.exit(ExitStatus.FAILED.value());
        }
        System.exit(returnCode);
    }
}
