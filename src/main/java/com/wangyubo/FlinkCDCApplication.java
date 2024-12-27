package com.wangyubo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * FlinkCDCApplication
 *
 * @author wyb
 * @since 2024/12/27 15:48
 */
public class FlinkCDCApplication {
    public static void main(String[] args) throws Exception {
        try {
            // 检查数据库连接是否成功
            checkDatabaseConnection("localhost", 3306, "cdc_test", "root", "123456");
        } catch (RuntimeException e) {
            System.err.println("程序终止：数据库连接失败，无法继续执行 CDC 任务。");
            e.printStackTrace();
            return; // 直接终止 main 方法
        }

        // 1.写flink作业要干什么？初始化env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.做流作业，设置ck参数
        env.enableCheckpointing(60000L);
        env.setParallelism(1);

        // 3.配置和实例化 MySqlSource 数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")  // 设置 MySQL 主机地址
                .port(3306)             // 设置 MySQL 端口号
                .databaseList("cdc_test") // 设置捕获的数据库名,可传入多个
                .tableList("cdc_test.cdc_table") // 设置捕获的表名（格式：schema.table）可传入多个
                .username("root")       // 设置数据库用户名
                .password("123456")     // 设置数据库密码
                .includeSchemaChanges(true) // 是否捕获表结构变更（例如新增列）
                .startupOptions(StartupOptions.initial()) // 启动位置配置：从表的历史快照开始读取
                .deserializer(new JsonDebeziumDeserializationSchema()) // 指定反序列化器，解析数据为 JSON
                .build();               // 构建 MySqlSource 实例

        // 4.从执行环境注册MySqlSource，生成流计算的执行图
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql-CDC")
                .print(); // 5.接sink才能知道source里面来的那些数据

        env.execute();
    }

    /**
     * 检查数据库连接是否成功
     *
     * @param host     数据库主机地址
     * @param port     数据库端口
     * @param database 数据库名称
     * @param user     用户名
     * @param password 密码
     */
    private static void checkDatabaseConnection(String host, int port, String database, String user, String password) {
        String url = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("成功连接到 MySQL 数据库！");
        } catch (SQLException e) {
            throw new RuntimeException("无法连接到 MySQL 数据库，请检查配置！", e);
        }
    }
}
