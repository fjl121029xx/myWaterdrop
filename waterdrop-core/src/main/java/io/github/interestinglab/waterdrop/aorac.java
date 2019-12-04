package io.github.interestinglab.waterdrop;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

import java.sql.*;
import java.util.Properties;

public class aorac {


    
    // final static String DB_URL = "jdbc:oracle:thin:@myhost:1521/myorcldbservicename";
//     final static String DB_URL="jdbc:oracle:thin:@wallet_dbname?TNS_ADMIN=/Users/test/wallet_dbname";

    final static String DB_URL = "jdbc:oracle:thin:@douyuedb_high?TNS_ADMIN=D:\\oracle\\wallet_DOUYUEDB";
    final static String DB_USER = "hlletl";
    final static String DB_PASSWORD = "Welcome12345";

    public static void main(String[] args) throws SQLException {


        Properties info = new Properties();
        info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
        info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
        info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");


        OracleDataSource ods = new OracleDataSource();
        ods.setURL(DB_URL);
        ods.setConnectionProperties(info);
        System.out.println(ods);

        try (OracleConnection connection = (OracleConnection) ods.getConnection()) {

            DatabaseMetaData dbmd = connection.getMetaData();
            System.out.println("Driver Name: " + dbmd.getDriverName());
            System.out.println("Driver Version: " + dbmd.getDriverVersion());
            System.out.println("Default Row Prefetch Value is: " +
                    connection.getDefaultRowPrefetch());
            System.out.println("Database Username is: " + connection.getUserName());
            System.out.println();

        }
    }

}
