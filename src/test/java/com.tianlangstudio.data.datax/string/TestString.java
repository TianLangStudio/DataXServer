package esun.fbi.string;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by zhuhq on 2015/12/10.
 */
public class TestString {
    @Test
    public void testContains() {
        Assert.assertTrue("ssss\ndddd".contains("\n"));
    }
}
