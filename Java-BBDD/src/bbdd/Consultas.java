/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bbdd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 *
 * @author infor04
 */
public class Consultas {

    private final static Scanner leer = new Scanner(System.in);
    private final static String database = "jdbc:mysql://192.168.56.101:3306/beer";
    private final static String user = "alumne";
    private final static String password = "alualualu";

    /**
     * @return the database
     */
    public static String getDatabase() {
        return database;
    }

    /**
     * @return the user
     */
    public static String getUser() {
        return user;
    }

    /**
     * @return the password
     */
    public static String getPassword() {
        return password;
    }

    
    public static void selectDB() {
        boolean salir = false;
        while (!salir) {
            switch (pedirConsulta()) {
                case 1:
                    noPK_noPrepStatement();
                    break;
                case 2:
                    //noPK_PrepStatement();
                    break;
                case 3:
                    //PK_noPrepStatement();
                    break;
                case 4:
                    //PK_PrepStatement();
                    break;
                case 5:
                    salir = true;
                    break;
                default:
                    System.out.println("Opci\u00f3n no v\u00e1lida");
            }
        }
    }

    private static int pedirConsulta() {
        System.out.println("---------------------------");
        System.out.println("1) Consultar datos NO PK (SIN prepared Statement)");
        System.out.println("2) Consultar datos NO PK (CON prepared Statement)");
        System.out.println("3) Consultar datos PK (SIN prepared Statement)");
        System.out.println("4) Consultar datos PK (CON prepared Statement)");
        System.out.println("5) Volver");
        System.out.println("---------------------------");
        System.out.println("Introduce una opci\u00f3n: ");
        return leer.nextInt();
    }

    public static void noPK_noPrepStatement() {
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            Statement st = con.createStatement();
            DatabaseMetaData dbmd = con.getMetaData();
            mostrarTablas(dbmd);
            System.out.println("¿En que tabla quieres consultar? ");
            String tabla = leer.next();
        } catch (SQLException ex) {
            ex.getMessage();
            ex.getLocalizedMessage();
        }
    }

    private static void mostrarTablas(DatabaseMetaData dbmd) throws SQLException {
        String[] table = {"TABLE"};
        ResultSet tables = dbmd.getTables(null, null, null, table);
        System.out.println("TABLAS DISPONIBLES:");
        System.out.print("| ");
        while (tables.next()) {
            System.out.print(tables.getString(3)+" | ");
        }
        System.out.println("");
    }
    
    //public static void noPK_PrepStatement(){}
    //public static void PK_noPrepStatement(){}
    //public static void PK_PrepStatement(){}
    
    public static void updateDB() {
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("select * from Bar");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "   " + rs.getString(2));
            }
        } catch (SQLException ex) {
            ex.getMessage();
            ex.getLocalizedMessage();
        }
    }

    //public static void insertDB(){}
}
