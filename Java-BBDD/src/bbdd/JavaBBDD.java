/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bbdd;

import java.sql.*;
import java.util.Scanner;

/**
 *
 * @author infor04
 */
public class JavaBBDD {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        boolean salir = false;
        while (!salir) {
            switch (pedirOpcion()) {
                case 1:
                    selectBD();
                    break;
                case 2:
                    //updateBD();
                    break;
                case 3:
                    //insertBD();
                    break;
                case 4:
                    salir = true;
                    break;
                default:
                    System.out.println("Opción no válida");
            }
        }
    }

    public static int pedirOpcion() {
        Scanner leer = new Scanner(System.in);
        System.out.println("---------------------------");
        System.out.println("1) Consultar datos");
        System.out.println("2) Actualizar datos");
        System.out.println("3) Insertar datos");
        System.out.println("4) Salir");
        System.out.println("---------------------------");
        System.out.println("Introduce una opción: ");
        return leer.nextInt();
    }

    public static void selectBD() {
        boolean salir = false;
        while (!salir) {
            switch (pedirConsulta()) {
                case 1:
                    selectBD();
                    break;
                case 2:
                    //updateBD();
                    break;
                case 3:
                    //insertBD();
                    break;
                case 4:
                    salir = true;
                    break;
                default:
                    System.out.println("Opción no válida");
            }
        }
        try (Connection con = DriverManager.getConnection("jdbc:mysql://192.168.56.101:3306/beer", "alumne", "alualualu");
                Statement st = con.createStatement();
                ResultSet rs = st.executeQuery("select * from Bar")) {
            while (rs.next()) {
                System.out.println(rs.getString(1) + "   " + rs.getString(2));
            }
        } catch (SQLException ex) {
            ex.getMessage();
            ex.getLocalizedMessage();
        }

    }

    public static int pedirConsulta() {
        Scanner leer = new Scanner(System.in);
        System.out.println("---------------------------");
        System.out.println("1) Consultar datos");
        System.out.println("2) Actualizar datos");
        System.out.println("3) Insertar datos");
        System.out.println("4) Salir");
        System.out.println("---------------------------");
        System.out.println("Introduce una opción: ");
        return leer.nextInt();
    }

}
