/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bbdd;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author infor04
 */
public class Consultas {

    private final static Scanner leer = new Scanner(System.in);
    private final static String lineSeparator = System.getProperty("line.separator");
    private final static String user = "alumne";
    private final static String password = "alualualu";
    //private final static String database = "jdbc:mysql://192.168.56.101:3306/beer";
    //private final static String f_salida="C:\\Users\\infor04\\Desktop\\consultas.txt";
    private final static String database = "jdbc:mysql://db4free.net:3306/programacio";
    private final static String f_salida = "C:\\Users\\navar\\OneDrive\\Escritorio\\consultas.txt";

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
    public static void selectDB() throws SQLException, IOException {
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
    private static void noPK_noPrepStatement() throws SQLException, IOException {
        String tabla;
        String consulta;
        ResultSet rs = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword());
                FileWriter txt = new FileWriter(f_salida, true);
                Statement st = con.createStatement();) {

            DatabaseMetaData dbmd = con.getMetaData();
            System.out.print("¿Qué tabla quieres consultar? ");
            tabla = pedirTabla(dbmd); // pide una tabla al usuario
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                consulta = pedirConsulta(dbmd, tabla); // pide una consulta al usuario
                // si consulta="" no habrá cláusula WHERE en la query
                rs = st.executeQuery("select * from " + tabla + (("".equals(consulta)) ? consulta : (" where " + consulta))); // ejecuta la query
                txt.write("select * from " + tabla + (("".equals(consulta)) ? consulta : (" where " + consulta)) + lineSeparator); // añadimos la consulta al fichero
                mostrarResultado(rs, txt); // muestra el resultado

            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } finally {

        }

    }

    /**
     * Realiza una consulta que puede devolver varias filas de una tabla.
     *
     * Si no se especifica ningun campo mostrará toda la tabla
     *
     * Usando el "Prepared Statement"
     */
    private static void noPK_PrepStatement() throws SQLException, IOException {
        String tabla;
        String columna;
        String query;
        PreparedStatement pst = null;
        ResultSet rs = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword());
                FileWriter txt = new FileWriter(f_salida, true)) {
            DatabaseMetaData dbmd = con.getMetaData();

            System.out.print("¿Qué tabla quieres consultar? ");
            tabla = pedirTabla(dbmd); // pide una tabla al usuario
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                leer.nextLine();
                columna = pedirColumna(dbmd, tabla); // pide una columna al usuario
                if (!"".equals(columna)) { // comprobamos si se ha introducido una columna
                    query = "select * from " + tabla + " where " + columna + "=?"; // query con cláusua WHERE
                    pst = con.prepareStatement(query);
                    System.out.print("Introduce la búsqueda: ");
                    pst.setString(1, leer.nextLine()); // pide el valor de la columna a buscar
                } else { // si no se ha introducido ninguna columna mostrará toda la tabla
                    query = "select * from " + tabla; // query sin cláusula WHERE
                    pst = con.prepareStatement(query);
                }
                rs = pst.executeQuery(); //ejecuta la query
                txt.write(pst.toString() + lineSeparator); // añadimos la consulta al fichero
                mostrarResultado(rs, txt); //muestra el resultado
            } else {
                throw new SQLException("La tabla " + tabla + "no existe");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (pst != null) {
                pst.close();
            }
        }

    }

    /**
     * Realiza una consulta sólo sobre la PK de una tabla.
     *
     * No se usa el "Prepared Statement"
     *
     */
    private static void PK_noPrepStatement() throws SQLException, IOException {
        String tabla;
        String consulta = " where ";
        ResultSet rs = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword());
                FileWriter txt = new FileWriter(f_salida, true);
                Statement st = con.createStatement()) {

            DatabaseMetaData dbmd = con.getMetaData();
            System.out.print("¿Qué tabla quieres consultar? ");
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
                rs = st.executeQuery("select * from " + tabla + consulta);
                txt.write("select * from " + tabla + consulta + lineSeparator); // añadimos la consulta al fichero
                mostrarResultado(rs, txt);
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    /**
     * Realiza una consulta sólo sobre la PK de una tabla.
     *
     * Usando el "Prepared Statement"
     *
     * @throws SQLException
     * @throws IOException
     */
    private static void PK_PrepStatement() throws SQLException, IOException {
        String tabla;
        String consulta = "";
        PreparedStatement pst = null;
        ResultSet rs = null;
        ResultSet pk = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword());
                FileWriter txt = new FileWriter(f_salida, true)) {
            DatabaseMetaData dbmd = con.getMetaData();
            System.out.print("¿Qué tabla quieres consultar? ");
            tabla = pedirTabla(dbmd);
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe
                pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla
                consulta = prepararConsulta(pk);
                pst = con.prepareStatement("select * from " + tabla + " where " + consulta);

                System.out.println("Introduce la búsqueda de los siguientes campos: ");
                pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla
                int i = 1;
                leer.nextLine();
                while (pk.next()) { //recorremos las PKs para que el usuario indique la búsqueda
                    System.out.print(pk.getString(4) + " = ");
                    pst.setString(i, leer.nextLine());
                    i++;
                }
                rs = pst.executeQuery();
                txt.write(pst.toString() + lineSeparator); // añadimos la consulta al fichero
                mostrarResultado(rs, txt);
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (pk != null) {
                pk.close();
            }
            if (pst != null) {
                pst.close();
            }
        }
    }

    /**
     * Modifica una fila de una tabla
     *
     * @throws SQLException
     * @throws IOException
     */
    public static void updateDB() throws SQLException, IOException {
        String tabla;
        String consulta = "";
        String columna;
        ResultSet pk = null;
        PreparedStatement pst = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword());
                FileWriter txt = new FileWriter(f_salida, true)) {
            DatabaseMetaData dbmd = con.getMetaData();
            System.out.print("¿Qué tabla quieres modificar? ");
            tabla = pedirTabla(dbmd);
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe

                pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla                
                consulta = prepararConsulta(pk); //preparamos la consulta del statement
                leer.nextLine();
                columna = pedirColumna(dbmd, tabla); // pide una columna al usuario que modifacará

                pst = con.prepareStatement("update " + tabla + " set " + columna + "=?" + " where " + consulta);

                System.out.println("Introduce el nuevo valor: ");
                System.out.print(columna + "= ");
                pst.setString(1, leer.nextLine());

                System.out.println("Introduce la PK sobre la que hacer la modificación: ");
                pk = dbmd.getPrimaryKeys(null, null, tabla); // guardamos las PKs de la tabla
                int i = 2;
                while (pk.next()) { //recorremos las PKs para que el usuario indique la búsqueda
                    System.out.print(pk.getString(4) + "= ");
                    pst.setString(i, leer.nextLine());
                    i++;
                }
                pst.executeUpdate(); //UPDATE
                txt.write(pst.toString() + lineSeparator + lineSeparator); // añadimos la consulta al fichero
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } finally {
            if (pk != null) {
                pk.close();
            }
            if (pst != null) {
                pst.close();
            }
        }
    }

    /**
     * Inserte una fila en una tabla
     *
     * @throws SQLException
     * @throws IOException
     */
    public static void insertDB() throws SQLException, IOException {
        String tabla;
        String values = "";
        ResultSet columnas = null;
        PreparedStatement pst = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword());
                FileWriter txt = new FileWriter(f_salida, true)) {
            DatabaseMetaData dbmd = con.getMetaData();
            System.out.print("¿En qué tabla quieres insertar? ");
            tabla = pedirTabla(dbmd);
            if (dbmd.getTables(null, null, tabla, null).next()) { // comprobamos si la tabla introducida existe

                columnas = dbmd.getColumns(null, null, tabla, null); // guardamos las columnas de la tabla                
                values = prepararInsert(columnas); //preparamos el insert del prepareStatement

                pst = con.prepareStatement("insert into " + tabla + " values (" + values + ")");

                leer.nextLine();
                System.out.println("Introduce los valores a insertar: ");
                columnas = dbmd.getColumns(null, null, tabla, null);
                int i = 1;
                while (columnas.next()) { //recorremos las columnas para que el usuario indique los valores
                    System.out.print(columnas.getString(4) + "= ");
                    pst.setString(i, leer.nextLine());
                    i++;
                }
                pst.executeUpdate(); //UPDATE
                txt.write(pst.toString() + lineSeparator + lineSeparator); // añadimos la consulta al fichero
            } else {
                throw new SQLException("Table \'programacio." + tabla + "\' doesn't exist");
            }
        } finally {
            if (columnas != null) {
                columnas.close();
            }
            if (pst != null) {
                pst.close();
            }
        }

    }

    /**
     * Muestra al usuario las tablas disponibles Pide que se introduzca una
     * tabla
     *
     * @return Devuelve el nombre de la tabla introducido por el usuario
     */
    private static String pedirTabla(DatabaseMetaData dbmd) throws SQLException {
        mostrarTablas(dbmd);
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
        return leer.nextLine();
    }

    /**
     * Recorre las Pks de una tabla y devuelve el Statement de la cláusula WHERE
     *
     * @param pk PKs de una tabla
     * @return Devuelve el contenido que tendrá la cláusula WHERE del
     * PreparedStatement
     * @throws SQLException
     */
    private static String prepararConsulta(ResultSet columnas) throws SQLException {
        String consulta = "";
        boolean primera = true;
        while (columnas.next()) { //recorremos las PKs para preparar el statement
            if (primera) {
                consulta = columnas.getString(4) + "=?";
                primera = false;
            } else {
                consulta = consulta + " and " + columnas.getString(4) + "=?";
            }
        }
        return consulta;
    }

    private static String prepararInsert(ResultSet columnas) throws SQLException {
        String valores = "";
        boolean primera = true;
        while (columnas.next()) { //recorremos las PKs para preparar el statement
            if (primera) {
                valores = "?";
                primera = false;
            } else {
                valores = valores + ", ?";
            }
        }
        return valores;
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
    private static void mostrarResultado(ResultSet rs, FileWriter txt) throws SQLException, IOException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int rowcount = 0;
        if (rs.last()) { // contamos el número de filas del resultado
            rowcount = rs.getRow();
            rs.beforeFirst();
        }
        int n_col = rsmd.getColumnCount();
        System.out.println("\nCONSULTA: ");

        while (rs.next()) {
            for (int i = 1; i < n_col + 1; i++) {
                System.out.print(" | " + rs.getString(i));
                txt.write(" | " + rs.getString(i));
            }
            System.out.println(" |");
            txt.write(" |" + lineSeparator);
        }
        txt.write(lineSeparator);
        System.out.println(rowcount + " entradas encontradas");
        System.out.println("\n");
    }

    public static void transacciones() throws SQLException {
        boolean salir = false;
        while (!salir) {
            switch (menuTransaccion()) {
                case 1:
                    actualizacion_simple();
                    break;
                case 2:
                    transaccion_1();
                    break;
                case 3:
                    transaccion_2();
                    break;
                case 4:
                    salir = true;
                    break;
                default:
                    System.out.println("Opci\u00f3n no v\u00e1lida");
            }
        }
    }

    private static int menuTransaccion() {
        System.out.println("------------------------------------------------------");
        System.out.println("1) Actualización Simple");
        System.out.println("2) Transacción 1");
        System.out.println("3) Transacción 2");
        System.out.println("4) Volver");
        System.out.println("------------------------------------------------------");
        System.out.print("Introduce una opci\u00f3n: ");
        return leer.nextInt();
    }

    /**
     * Si falla la primera actualización no se actualizará la tabla pero si
     * falla la segunda actualización, la primera actualización quedará guardada
     *
     * @throws SQLException
     */
    public static void actualizacion_simple() throws SQLException {
        PreparedStatement pst = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {

            DatabaseMetaData dbmd = con.getMetaData();
            for (int i = 0; i < 2; i++) { // 2 actualizaciones
                System.out.println("Introduce la columna a modificar: ");
                String columna = pedirColumna(dbmd, "Beer");
                pst = con.prepareStatement("update Beer set " + columna + "=? where name=?");
                System.out.println("Introduce el nuevo valor: ");
                pst.setString(1, leer.nextLine());
                System.out.println("Introduce el nombre de la cerveza a modificar: ");
                pst.setString(2, leer.nextLine());
                pst.executeUpdate(); //UPDATE
            }
        } finally {
            if (pst != null) {
                pst.close();
            }
        }

    }

    /**
     * Si falla durante las 3 primeras actualizaciones deshará las
     * actualizaciones hechas Si falla en la actualización que está fuera de la
     * transacción las 3 primeras quedarán guardadas
     *
     * @throws SQLException
     */
    public static void transaccion_1() throws SQLException {
        PreparedStatement pst = null;
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            DatabaseMetaData dbmd = con.getMetaData();
            boolean estado_anterior = con.getAutoCommit();
            String columna;
            try { // INICIO TRANSACCIÓN
                con.setAutoCommit(false);
                leer.nextLine();

                for (int i = 1; i < 4; i++) { // 3 updates en la transacción
                    System.out.println("UP " + i + ": Introduce la columna a modificar: ");
                    columna = pedirColumna(dbmd, "Beer");
                    pst = con.prepareStatement("update Beer set " + columna + "=? where name=?");
                    System.out.println("Introduce el nuevo valor: ");
                    pst.setString(1, leer.nextLine());
                    System.out.println("Introduce el nombre de la cerveza a modificar: ");
                    pst.setString(2, leer.nextLine());
                    pst.executeUpdate();
                }
                con.commit(); // commit tras las 3 actualizaciones
            } catch (SQLException ex) {
                System.out.println("Error en transacción");
                con.rollback();
            } finally {
                con.setAutoCommit(estado_anterior);
            } //FIN TRANSACCIÓN
            //Última actualización fuera de la transacción
            System.out.println("UP 4 (Fuera de transacción): Introduce la columna a modificar: ");
            columna = pedirColumna(dbmd, "Beer");
            pst = con.prepareStatement("update Beer set " + columna + "=? where name=?");
            System.out.println("Introduce el nuevo valor: ");
            pst.setString(1, leer.nextLine());
            System.out.println("Introduce el nombre de la cerveza a modificar: ");
            pst.setString(2, leer.nextLine());
            pst.executeUpdate(); //UPDATE
        } finally {
            if (pst != null) {
                pst.close();
            }
        }

    }

    public static void transaccion_2() throws SQLException {
        try (final Connection con = DriverManager.getConnection(getDatabase(), getUser(), getPassword())) {
            DatabaseMetaData dbmd = con.getMetaData();
            boolean estado_anterior = con.getAutoCommit();
            String columna;
            try { // INICIO TRANSACCIÓN
                con.setAutoCommit(false);
                leer.nextLine();
                Savepoint save;

                for (int i = 1; i < 4; i++) { // 3 updates en la transacción
                    System.out.println("UP " + i + ": Introduce la columna a modificar: ");
                    columna = pedirColumna(dbmd, "Beer");
                    PreparedStatement pst = con.prepareStatement("update Beer set " + columna + "=? where name=?");
                    System.out.println("Introduce el nuevo valor: ");
                    pst.setString(1, leer.nextLine());
                    System.out.println("Introduce el nombre de la cerveza a modificar: ");
                    pst.setString(2, leer.nextLine());
                    pst.executeUpdate();
                    if (i == 2) {
                        save = con.setSavepoint(); //Savepoint en el segundo update
                    }
                }
                con.commit(); // commit tras las 3 actualizaciones
            } catch (SQLException ex) {
                System.out.println("Error en transacción");
                con.rollback();
            } finally {
                con.setAutoCommit(estado_anterior);
            } //FIN TRANSACCIÓN
            //Última actualización fuera de la transacción
            System.out.println("UP 4 (Fuera de transacción): Introduce la columna a modificar: ");
            columna = pedirColumna(dbmd, "Beer");
            PreparedStatement pst = con.prepareStatement("update Beer set " + columna + "=? where name=?");
            System.out.println("Introduce el nuevo valor: ");
            pst.setString(1, leer.nextLine());
            System.out.println("Introduce el nombre de la cerveza a modificar: ");
            pst.setString(2, leer.nextLine());
            pst.executeUpdate(); //UPDATE
        }
    }
}
