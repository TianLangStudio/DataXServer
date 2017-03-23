package esun.fbi.map;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

/**
 * Created by zhuhq on 2016/5/5.
 */
public class TestMap {
    @Test
    public void testRemove() {
        Map<String,String> map = new HashMap<String,String>();
        map.put("a","1");
        map.put("b","2");
        map.put("c","1");
        map.put("d","2");
        map.put("e","1");
        map.put("f","2");
        //java.util.ConcurrentModificationException
        /*for(Map.Entry entry : map.entrySet()) {
            map.remove(entry.getKey());
        }*/
       /*
        //java.util.ConcurrentModificationException
       Iterator<Map.Entry<String,String>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            map.remove(iterator.next().getKey());
        }*/
    }
}
