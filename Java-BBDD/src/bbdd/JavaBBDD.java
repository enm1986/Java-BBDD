/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bbdd;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

/**
 * Práctica de BBDD en Java
 *
 * @author Eugenio Navarro
 */
public class JavaBBDD {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here

        boolean salir = false;
        while (!salir) {
            try {
                switch (pedirOpcion()) {
                    case 1:
                        Consultas.selectDB();
                        break;
                    case 2:
                        Consultas.updateDB();
                        break;
                    case 3:
                        Consultas.insertDB();
                        break;
                    case 4:
                        Consultas.transacciones();
                        break;
                    case 5:
                        salir = true;
                        break;
                    default:
                        System.out.println("Opción no válida");
                }

            } catch (SQLException ex) {
                System.out.println(ex.getSQLState());
                System.out.println(ex.getMessage());
                System.out.println(Arrays.toString(ex.getStackTrace()));
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
                System.out.println(Arrays.toString(ex.getStackTrace()));    
            }
        }
    }

    public static int pedirOpcion() {
        Scanner leer = new Scanner(System.in);
        System.out.println("------------------------------------------------------");
        System.out.println("1) Consultar datos");
        System.out.println("2) Actualizar datos");
        System.out.println("3) Insertar datos");
        System.out.println("4) TRANSACCIONES");
        System.out.println("5) Salir");
        System.out.println("------------------------------------------------------");
        System.out.print("Introduce una opción: ");
        return leer.nextInt();
    }

}
