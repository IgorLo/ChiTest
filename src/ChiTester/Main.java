package ChiTester;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.apache.commons.math3.*;
import org.apache.commons.math3.stat.inference.ChiSquareTest;

public class Main {

    public static int GLOBAL_COUNTER = 0;
    public static int TOTAL_LINES_TO_PROCCESS;

    public static int TIME_SOUT_COUNTER = 0;
    public static int TIME_SOUT_LIMIT;
    public static long startTime = System.currentTimeMillis();

    private static ChiSquareTest test = new ChiSquareTest();

    //args[0] - index of 1 chromosom
    //args[1] - index of 2 ch
    //args[2] - write time every N lines
    //args[3] - total lines to proccess (0 if dont stop)

    public static void main(String[] args) {

        TOTAL_LINES_TO_PROCCESS = Integer.parseInt(args[3]);

        TIME_SOUT_LIMIT = Integer.parseInt(args[2]);

        Connection SQConnection = connect();
        runTests(SQConnection, args[0], args[1]);

    }


    public static Connection connect() {
        Connection conn = null;
        try {
            // db parameters
            String url = "jdbc:sqlite:biohack.db";
            // create a connection to the database
            conn = DriverManager.getConnection(url);

            System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    public static void createNewTable(Connection connection, String index1, String index2) {
        // SQLite connection string
        // SQL statement for creating a new table
        String sql = "CREATE TABLE IF NOT EXISTS, chisquare" + index1 + index2 + " (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	id text NOT NULL,\n"
                + "	id" + index1 + "integer,\n"
                + " id" + index2 + "integer,\n"
                + " pval real\n"
                + ");";
        try (Statement stmt = connection.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void runTests(Connection conn, String index1, String index2) {
        String sql1 = "SELECT id, ch, pos, ref, alt, info, humans FROM chrom" + index1;
        String sql2 = "SELECT id, ch, pos, ref, alt, info, humans FROM chrom" + index2;

        try (Statement stmt1 = conn.createStatement();
             Statement stmt2 = conn.createStatement()) {
            ResultSet rs1 = stmt1.executeQuery(sql1);
            while (rs1.next()) {
                String mutationName1 =
                        "ID=" + rs1.getInt("id")
                                + ":"
                                + Integer.toString(rs1.getInt("ch"))
                                + ":"
                                + Integer.toString(rs1.getInt("pos"))
                                + ":"
                                + rs1.getString("ref")
                                + ":"
                                + rs1.getString("alt")
                                + "(" + rs1.getString("info") + ")";

                String h1 = rs1.getString("humans");

                ResultSet rs2 = stmt2.executeQuery(sql2);

                while (rs2.next()) {
                    String mutationName2 =
                            "ID=" + rs2.getInt("id")
                                    + ":"
                                    + Integer.toString(rs2.getInt("ch"))
                                    + ":"
                                    + Integer.toString(rs2.getInt("pos"))
                                    + ":"
                                    + rs2.getString("ref")
                                    + ":"
                                    + rs2.getString("alt")
                                    + "(" + rs2.getString("info") + ")";

                    String h2 = rs2.getString("humans");

                    long[][] matrix = new long[3][3];

                    for (int i = 0; i < h2.length(); i++) {
                        matrix[h1.charAt(i) - '0'][h2.charAt(i) - '0']++;
                    }

                    /*
                    System.out.println("-------------");
                    System.out.println(matrix[0][0] + " " + matrix[0][1] + " " + matrix[0][2]);
                    System.out.println(matrix[1][0] + " " + matrix[1][1] + " " + matrix[1][2]);
                    System.out.println(matrix[2][0] + " " + matrix[2][1] + " " + matrix[2][2]);
                    */

                    Double testResult = test.chiSquareTest(matrix);

                    GLOBAL_COUNTER++;
                    if (TOTAL_LINES_TO_PROCCESS != 0) {
                        if (GLOBAL_COUNTER >= TOTAL_LINES_TO_PROCCESS) {
                            System.out.println(TOTAL_LINES_TO_PROCCESS + " pairs proceeded");
                            System.exit(1);
                        }
                    }
                    TIME_SOUT_COUNTER++;

                    if (!testResult.isNaN()) {
                        String result = (mutationName1 + " - "
                                + mutationName2
                                + ", p-value=" + Double.toString(testResult) + "\n");
                        writeResultsToFile(index1, index2, result);
                    }

                    if (TIME_SOUT_COUNTER == TIME_SOUT_LIMIT) {
                        TIME_SOUT_COUNTER = 0;
                        writeWorkTime();
                    }

                }

                rs2.close();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void writeWorkTime() {
        System.out.println("-----");
        System.out.println("Lines: " + GLOBAL_COUNTER);
        System.out.println("Seconds spent: " + Long.toString((System.currentTimeMillis() - startTime) / 1000));
    }

    private static void writeResultsToFile(String index1, String index2, String line) {

        String sql = "INSERT INTO warehouses(name,capacity) VALUES(?,?)";

        try (Connection conn = this.connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setDouble(2, capacity);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        System.out.println(line);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter("ChiTestOut/ChiTestFor_ch" + index1 + "_ch" + index2 + ".txt", true));
            writer.write(line);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null)
                    writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
