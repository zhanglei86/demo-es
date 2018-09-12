package win.leizhang.demo.es.demoes.test;

import com.alibaba.fastjson.JSON;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringRunner;
import win.leizhang.demo.es.demoes.DemoEsApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {DemoEsApplication.class})
public class BaseTestCase extends AbstractJUnit4SpringContextTests {

    public final Logger log = LoggerFactory.getLogger(this.getClass());

    private long beginTime;
    private long endTime;

    static {
        // server端口
        System.setProperty("server.port", "5012");
        System.setProperty("management.port", "5013");
    }

    @Before
    public void begin() {
        beginTime = System.currentTimeMillis();
    }

    @After
    public void end() {

        endTime = System.currentTimeMillis();

        System.err.println("");
        System.err.println("#######################################################");
        System.err.println("elapsed time : " + (endTime - beginTime) + "ms");
        System.err.println("#######################################################");
        System.err.println("");
    }

    public void printData(Object data) {
        String str;
        if (data instanceof String) {
            str = String.valueOf(data);
        } else {
            str = JSON.toJSONString(data);
        }
        System.err.println("data ==> " + str);
    }

    @Test
    public void testCase() {
        System.out.println("base testCase finish!");
    }

}
