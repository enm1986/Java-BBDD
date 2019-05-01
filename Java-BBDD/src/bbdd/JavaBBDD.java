/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bbdd;

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
                    salir = true;
                    break;
                default:
                    System.out.println("Opción no válida");
            }
        }
    }

    public static int pedirOpcion() {
        Scanner leer = new Scanner(System.in);
        System.out.println("------------------------------------------------------");
        System.out.println("1) Consultar datos");
        System.out.println("2) Actualizar datos");
        System.out.println("3) Insertar datos");
        System.out.println("4) Salir");
        System.out.println("------------------------------------------------------");
        System.out.print("Introduce una opción: ");
        return leer.nextInt();
    }

}
