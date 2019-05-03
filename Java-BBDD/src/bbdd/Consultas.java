/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bbdd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/**
 *
 * @author infor04
 */
public class Consultas {

    private final static Scanner leer = new Scanner(System.in);
    private final static String user = "alumne";
    private final static String password = "alualualu";
    private final static String database = "jdbc:mysql://192.168.56.101:3306/beer";
    //private final static String database = "jdbc:mysql://db4free.net:3306/programacio";

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

    //MÉTODOS
    /**
     * Método de CONSULTA "SELECT". Pide al usuario que tipo de consulta quiere
     * hacer
     */
    public static void selectDB() {
        boolean salir = false;
        while (!salir) {
            switch (pedirOpcion()) {
                case 1:
                    noPK_noPrepStatement();
                    break;
                case 2:
                    noPK_PrepStatement();
                    break;
                case 3:
                    PK_noPrepStatement();
                    break;
                case 4:
                    PK_PrepStatement();
                    break;
                case 5:
                    salir = true;
                    break;
                default:
                    System.out.println("Opci\u00f3n no v\u00e1lida");
            }
        }
    }

    /**
     * Muestra un menú por pantalla de los tipos de consultas disponibles
     *
     * @return Devuelve el tipo de consulta a realizar
     */
    private static int pedirOpcion() {
        System.out.println("------------------------------------------------------");
        System.out.println("1) Consultar datos NO PK (SIN prepared Statement)");
        System.out.println("2) Consultar datos NO PK (CON prepared Statement)");
        System.out.println("3) Consultar datos PK (SIN prepared Statement)");
        System.out.println("4) Consultar datos PK (CON prepared Statement)");
        System.out.println("5) Volver");
        System.out.println("------------------------------------------------------");
        System.out.print("Introduce una opci\u00f3n: ");
        return leer.nextInt();
    }

    /**
     * Realiza una consulta que puede devolver varias filas de una tabla.
     *
     * Si no se especifica ningun campo mostrará toda la tabla
     *
     * No se usa el "Prepared Statement"
     */
    private static void noPK_noPrepStatement() {
        String tabla;
        String consulta;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            Statement st = con.createStatement();
            DatabaseMetaData dbmd = con.getMetaData();
            tabla = pedirTabla(dbmd); // pide una tabla al usuario
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                consulta = pedirConsulta(dbmd, tabla); // pide una consulta al usuario
                // si consulta="" no habrá cláusula WHERE en la query
                ResultSet rs = st.executeQuery("select * from " + tabla + (("".equals(consulta)) ? consulta : (" where " + consulta))); // ejecuta la query
                mostrarResultado(rs); // muestra el resultado
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } catch (SQLException ex) {
            System.out.println(ex.getSQLState());
            System.out.println(ex.getMessage());
            System.out.println(ex.getLocalizedMessage());
        }
    }

    /**
     * Realiza una consulta que puede devolver varias filas de una tabla.
     *
     * Si no se especifica ningun campo mostrará toda la tabla
     *
     * Usando el "Prepared Statement"
     */
    private static void noPK_PrepStatement() {
        String tabla;
        String columna;
        String query;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            DatabaseMetaData dbmd = con.getMetaData();
            PreparedStatement pst;
            tabla = pedirTabla(dbmd); // pide una tabla al usuario
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                columna = pedirColumna(dbmd, tabla); // pide una columna al usuario
                //System.out.println(dbmd.getColumns(null, null, tabla, columna).getMetaData().getColumnTypeName(1)); //para ver el tipo de la columna
                if (!"".equals(columna)) { // comprobamos si se ha introducido una columna
                    query = "select * from " + tabla + " where " + columna + "=?"; // query con cláusua WHERE
                    pst = con.prepareStatement(query);
                    System.out.print("Introduce la búsqueda: ");
                    pst.setString(1, leer.nextLine()); // pide el valor de la columna a buscar
                } else { // si no se ha introducido ninguna columna mostrará toda la tabla
                    query = "select * from " + tabla; // query sin cláusula WHERE
                    pst = con.prepareStatement(query);
                }
                ResultSet rs = pst.executeQuery(); //ejecuta la query
                mostrarResultado(rs); //muestra el resultado
            } else {
                throw new SQLException("La tabla " + tabla + "no existe");
            }
        } catch (SQLException ex) {
            System.out.println(ex.getSQLState());
            System.out.println(ex.getMessage());
            System.out.println(ex.getLocalizedMessage());
        }
    }

    /**
     * Realiza una consulta sólo sobre la PK de una tabla.
     *
     * No se usa el "Prepared Statement"
     */
    private static void PK_noPrepStatement() {
        String tabla;
        String consulta = " where ";
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            Statement st = con.createStatement();
            DatabaseMetaData dbmd = con.getMetaData();
            tabla = pedirTabla(dbmd);
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                ResultSet pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla
                System.out.println("Introduce la búsqueda de los siguientes campos: ");
                boolean primera = true;
                leer.nextLine();
                while (pk.next()) { //recorremos las PKs para realizar la consulta sobre ellas
                    System.out.print(pk.getString(4) + " = ");
                    if (primera) {
                        consulta = consulta + pk.getString(4) + "=\"" + leer.nextLine() + "\""; 
                        primera = false;
                    } else {
                        consulta = consulta + " and " + pk.getString(4) + "=\"" + leer.nextLine() + "\"";
                    }
                }
                ResultSet rs = st.executeQuery("select * from " + tabla + consulta);
                mostrarResultado(rs);
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } catch (SQLException ex) {
            System.out.println(ex.getSQLState());
            System.out.println(ex.getMessage());
            System.out.println(ex.getLocalizedMessage());
        }
    }

    /**
     * Realiza una consulta sólo sobre la PK de una tabla.
     *
     * Usando el "Prepared Statement"
     */
    private static void PK_PrepStatement() {
        String tabla;
        String consulta = "";
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            DatabaseMetaData dbmd = con.getMetaData();
            tabla = pedirTabla(dbmd);
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                ResultSet pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla
                boolean primera = true;
                while (pk.next()) { //recorremos las PKs para preparar el statement
                    if (primera) {
                        consulta = pk.getString(4) + "=?";
                        primera = false;
                    } else {
                        consulta = consulta + " and " + pk.getString(4) + "=?";
                    }
                }
                PreparedStatement pst = con.prepareStatement("select * from " + tabla + " where " + consulta);

                System.out.println("Introduce la búsqueda de los siguientes campos: ");
                pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla
                int i = 1;
                leer.nextLine();
                while (pk.next()) { //recorremos las PKs para que el usuario indique la búsqueda
                    System.out.print(pk.getString(4) + " = ");
                    pst.setString(i, leer.nextLine());
                    i++;
                }
                ResultSet rs = pst.executeQuery();
                mostrarResultado(rs);
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } catch (SQLException ex) {
            System.out.println(ex.getSQLState());
            System.out.println(ex.getMessage());
            System.out.println(ex.getLocalizedMessage());
        }
    }

    public static void updateDB() {
    }

    public static void insertDB() {
    }

    /**
     * Muestra al usuario las tablas disponibles Pide que se introduzca una
     * tabla
     *
     * @return Devuelve el nombre de la tabla introducido por el usuario
     */
    private static String pedirTabla(DatabaseMetaData dbmd) throws SQLException {
        mostrarTablas(dbmd);
        System.out.print("¿Qué tabla quieres consultar? ");
        return leer.next();
    }

    /**
     * Muestra al usuario las columnas disponibles de una tabla. Pide el
     * contenido de la cláusula 'WHERE' de una query SQL y la construye.
     *
     * @return Devuelve la cláusula 'WHERE' de la query construida
     */
    private static String pedirConsulta(DatabaseMetaData dbmd, String tabla) throws SQLException {
        mostrarColumnas(dbmd, tabla);
        System.out.println("Introduce la consulta SQL:");
        System.out.println("(Ejemplo: price<3)");
        System.out.println("(Ejemplo: name=\"Amy\")");
        System.out.println("(Si se deja vacío muestra todas las filas de la tabla)");
        leer.nextLine();
        return leer.nextLine();
    }

    /**
     * Muestra al usuario las columnas disponibles de una tabla. Pide que se
     * introduzca una columna
     *
     * @return Devuelve la cláusula 'WHERE' de la query construida
     */
    private static String pedirColumna(DatabaseMetaData dbmd, String tabla) throws SQLException {
        mostrarColumnas(dbmd, tabla);
        System.out.println("Introduce una columna: ");
        leer.nextLine();
        return leer.nextLine();
    }

    /**
     * Muestra por pantalla las tablas disponibles de la base de datos
     *
     * @param dbmd Metadatos de la base de datos conectada
     * @throws SQLException
     */
    private static void mostrarTablas(DatabaseMetaData dbmd) throws SQLException {
        ResultSet tables = dbmd.getTables(null, null, null, new String[]{"TABLE"});
        System.out.println("\nTABLAS DISPONIBLES:");
        while (tables.next()) {
            System.out.print("| " + tables.getString(3) + " ");
        }
        System.out.println("|\n");
    }

    /**
     * Muestra los nombres de las columnas de una tabla
     *
     * @param dbmd Metadatos de la base de datos
     * @param table Tabla de la que se quiere mostrar el nombre de sus columnas
     * @throws SQLException
     */
    private static void mostrarColumnas(DatabaseMetaData dbmd, String tabla) throws SQLException {
        ResultSet columnas = dbmd.getColumns(null, null, tabla, null);
        System.out.println("\nCOLUMNAS DE \"" + tabla + "\": ");
        while (columnas.next()) {
            System.out.print("| " + columnas.getString("COLUMN_NAME") + " ");
        }
        System.out.println("|\n");
    }

    /**
     * Muestra el resultado de una consulta SELECT
     *
     * @param rs Resultados de la consulta realizada
     * @throws SQLException
     */
    private static void mostrarResultado(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int n_col = rsmd.getColumnCount();
        System.out.println("\nCONSULTA: ");
        while (rs.next()) {
            for (int i = 1; i < n_col + 1; i++) {
                System.out.print(" | " + rs.getString(i));
            }
            System.out.println(" |");
        }
        System.out.println("\n");
    }
}
