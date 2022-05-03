/*
 * Created on 12.06.2006
 */
package smallsql.tools;

import csc468.WorkloadRecorder;
import smallsql.database.SSDriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

public class cli {

    public static void main(String... args) throws Exception {
        System.err.println(String.format("SmallSQL Database CLI <v%d.%d>%n", config.VERSION_NUMBER_MAJOR, config.VERSION_NUMBER_MINOR));

        Connection con = new SSDriver().connect("jdbc:smallsql:db1?create=true", new Properties());
        Statement st = con.createStatement();

        printHelp(con);

        Scanner sc = new Scanner(System.in);
        StringBuilder commandBuilder = new StringBuilder();
        WorkloadRecorder workloadRecorder = WorkloadRecorder.getInstance();

        while (true) {
            if (commandBuilder.length() == 0) {
                System.out.print("\nCommand (<q> to exit): ");
            }
            String currentLine = sc.nextLine();
            currentLine = currentLine == null ? "" : currentLine.trim();
            commandBuilder.append(currentLine).append('\n');

            // Commit current command
            if (currentLine.endsWith(";")) {
                String prep = commandBuilder.toString().trim();
                System.out.println("Executing...\n");
                try {
                    boolean hasOutput = st.execute(prep.substring(0, prep.length() - 1));
                    if (hasOutput) {
                        printRS(st.getResultSet());
                    }
                    workloadRecorder.writeQueryToWorkload(prep);
                } catch (SQLException e) {
                    System.err.println(e.getLocalizedMessage());
                } finally {
                    commandBuilder.setLength(0);
                }

            } else if (currentLine.equals("q") || currentLine.equals("Q")) {
                sc.close();
                break;
            }
        }
        st.close();
        workloadRecorder.closeWriter();
        System.err.println("Bye!");
    }

    private static void printHelp(Connection con) throws SQLException {
        System.out.println(String.format("Connection version: <%s>, current database: %s%n",
                con.getMetaData().getDatabaseProductVersion(),
                con.getCatalog()));
        System.out.println("Type 'USE' to change the database context.");
        System.out.println("Command end with ';' with be executed.");
    }

    private static void printRS(ResultSet rs) throws SQLException {
        System.out.println("Result:");
        ResultSetMetaData md = rs.getMetaData();
        int count = md.getColumnCount();
        for (int i = 1; i <= count; i++) {
            System.out.print(md.getColumnLabel(i));
            System.out.print('\t');
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <= count; i++) {
                System.out.print(rs.getObject(i));
                System.out.print('\t');
            }
            System.out.println();
        }
    }
}
