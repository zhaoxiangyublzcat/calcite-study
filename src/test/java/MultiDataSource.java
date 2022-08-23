import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MultiDataSource {
    Properties info = null;

    @Before
    public void before() throws UnsupportedEncodingException {
        URL url = MultiDataSource.class.getResource("/model.json");
        String str = URLDecoder.decode(url.toString(), "UTF-8");
        info = new Properties();
        info.put("model", str.replace("file:", ""));
        // 不区分大小写
        info.setProperty("caseSensitive","false");
    }

    @Test
    public void multiDataSourceSelect() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
        Statement stat = conn.createStatement();
        ResultSet res = stat.executeQuery("select * from custom_csv.TEST1");
        List<Map<String,Object>> list = Lists.newArrayList();
        ResultSetMetaData metaData = res.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (res.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), res.getObject(i));
            }
            list.add(map);
        }
        System.out.println(JSON.toJSONString(list));

        conn.close();
    }
}
