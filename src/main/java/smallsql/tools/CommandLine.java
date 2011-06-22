/*
 * Created on 12.06.2006
 */
package smallsql.tools;

import java.io.*;
import java.sql.*;
import java.util.Properties;

import javax.swing.JOptionPane;

import smallsql.database.*;


/**
 * @author Volker Berlin
 */
public class CommandLine {


    public static void main(String[] args) throws Exception {
        System.out.println("SmallSQL Database command line tool\n");
        Connection con = new SSDriver().connect("jdbc:smallsql", new Properties());
        Statement st = con.createStatement();
        if(args.length>0){
            con.setCatalog(args[0]);
        }
        System.out.println("\tVersion: "+con.getMetaData().getDatabaseProductVersion());
        System.out.println("\tCurrent database: "+con.getCatalog());
        System.out.println();
        System.out.println("\tUse the USE command to change the database context.");
        System.out.println("\tType 2 times ENTER to execute any SQL command.");
        
        StringBuffer command = new StringBuffer();
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            try {
                String line;
                try{
                    line = input.readLine();
                }catch(IOException ex){
                    ex.printStackTrace();
                    JOptionPane.showMessageDialog( null, "You need to start the command line of the \nSmallSQL Database with a console window:\n\n       java -jar smallsql.jar\n\n" + ex, "Fatal Error", JOptionPane.OK_OPTION);
                    return;
                }
                if(line == null){
                    return; //end of program
                }
                if(line.length() == 0 && command.length() > 0){
                    boolean isRS = st.execute(command.toString());
                    if(isRS){
                        printRS(st.getResultSet());
                    }
                    command.setLength(0);
                }
                command.append(line).append('\n');
            } catch (Exception e) {
                command.setLength(0);
                e.printStackTrace();
            }
        }
        
    }
    

    private static void printRS(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int count = md.getColumnCount();
        for(int i=1; i<=count; i++){
            System.out.print(md.getColumnLabel(i));
            System.out.print('\t');
        }
        System.out.println();
        while(rs.next()){
            for(int i=1; i<=count; i++){
                System.out.print(rs.getObject(i));
                System.out.print('\t');
            }
            System.out.println();
        }
    }
}
