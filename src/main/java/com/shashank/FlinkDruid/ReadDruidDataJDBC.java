package com.shashank.FlinkDruid;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class ReadDruidDataJDBC {

    public static void main(String[] args) {
        SpringApplication.run(ReadDruidDataJDBC.class, args);
        Logger log = LoggerFactory.getLogger("FlinkDruidApplication");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Row> dbData =
                env.createInput(
                        JDBCInputFormat
                                .buildJDBCInputFormat()
                                .setDrivername("org.apache.calcite.avatica.remote.Driver")
                                .setDBUrl("jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/")
                                .setUsername("null")
                                .setPassword("null")
                                .setQuery(
                                        "SELECT sourceIP FROM druid_database"
                                )
                                .setRowTypeInfo((RowTypeInfo) Types.ROW(Types.STRING))
                                .finish()
                );

        try {

            log.info("Printing first IP :: {} " + dbData.collect().iterator().next());
        } catch (Exception e) {
            log.error(e.getMessage());
        }


    }


}
