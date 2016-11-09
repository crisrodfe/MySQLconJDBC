
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author CrisRodFe
 */
public class Menu 
{
    private static Scanner sc = new Scanner(System.in);
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException 
    {
        int opcion = 0;
        Connection conn = crearConexion();
        do
        {
            System.out.println("\nElija la operación que quiere realizar:\n"
                             + "1.Mostrar información sobre la base de datos\n" +
                               "2.Mostrar información sobre las tablas\n" +
                               "3.Información sobre las columnas de cada tabla.\n" +
                               "4.Ejecutar sentencias de descripción de datos.\n" +
                               "5.Ejecutar sentencias de definicion(crear o borrar tablas).\n"+
                               "6.Ejecutar sentencia de manipulación de datos.\n"+
                               "7. Ejecutar procedimiento.\n" 
                             + "8.Salir al menú de BBDD.");
            opcion = Integer.parseInt(sc.nextLine());
            switch(opcion)
            {
                case 1:
                    informacionBBDD(conn);
                    break;
                case 2:
                    informacionTablas(conn);
                    break;
                case 3:
                    informacionColumnas(conn);
                    break;
                case 4:
                    ejecutarDescripcion(conn);
                    break;    
                case 5:                    
                    ejecutarDefinicion(conn);
                    break;
                case 6:
                    ejecutarManipulacion(conn);
                    break;  
                case 7:
                    ejecutarProcedimiento(conn);
                    break;
                case 8 :
                    System.exit(0);
                    break;
                default:    
                    System.out.println("Opción incorrecta.");
                    break;
            }        
        }while(opcion != 8);    
    }

    private static Connection  crearConexion() throws ClassNotFoundException, SQLException
    {
        Class.forName("com.mysql.jdbc.Driver");    
        Connection c = DriverManager.getConnection("jdbc:mysql://localhost/ejemplo","ejemplo","ejemplo");
        
        return c;
    }
    
    private static  void informacionBBDD(Connection conn) throws SQLException 
    {
        DatabaseMetaData dbmd=conn.getMetaData();
        ResultSet result = null;
        String nombre= dbmd.getDatabaseProductName();
        String driver= dbmd.getDriverName();
        String url= dbmd.getURL();
        String usuario= dbmd.getUserName();

        System.out.println("\nINFORMACIÓN BASE DATOS");
        System.out.println("Nombre: "+nombre);
        System.out.println("Driver: "+driver);
        System.out.println("URL: "+url);
        System.out.println("Usuario: "+usuario);  
    }

    private static void informacionTablas(Connection conn) throws SQLException 
    {
        DatabaseMetaData dbmd=conn.getMetaData();
        ResultSet result= dbmd.getTables(null,conn.getSchema(),null,null);
        
        while (result.next())
        {
            String catalogo= result.getString(1);
            String esquema= result.getString(2);
            String tabla= result.getString(3);
            String tipo= result.getString(4);
            System.out.println(tipo + " - Catálogo: "+ catalogo+", Esquema: "+esquema+", Nombre: "+tabla);
        }
        result.close();
    }

    private static void informacionColumnas(Connection conn) throws SQLException 
    {
        DatabaseMetaData dbmd=conn.getMetaData();    
        ResultSet columnas = dbmd.getColumns(null, conn.getSchema(), null, null);
        while (columnas.next())
        {
            String name= columnas.getString(3);
            String nombreCol= columnas.getString("COLUMN_NAME");
            String tipoCol= columnas.getString("TYPE_NAME");
            String tamCol= columnas.getString("COLUMN_SIZE");
            String nula= columnas.getString("IS_NULLABLE");
            System.out.println("Tabla: "+name+" Columna: "+nombreCol+", Tipo: "+tipoCol +", Tamaño: "+tamCol+", ¿Es nula?: "+ nula);
        } 
        
        columnas.close();
    }

    private static void ejecutarDescripcion(Connection conn) throws SQLException 
    {
        System.out.println("Introduzca la tabla sobre la que quiere ejecutar la consulta:(empleados o departamentos)");       
        String tabla = sc.nextLine();
        System.out.println("Introduzca la columna sobre la que quiere ejecutar la consulta:(apellido,dept_no,dnombre,etc)");       
        String columna = sc.nextLine();
        
        Statement sentencia= conn.createStatement();
        ResultSet rs= sentencia.executeQuery("SELECT "+columna+" FROM "+tabla);
        ResultSetMetaData rsmd= rs.getMetaData();
        int nCol= rsmd.getColumnCount();
        System.out.printf("%-20s%-15s%-8s%-5s\n", "NOMBRE", "TIPO", "NULA", "ANCHO");

        System.out.println("Número de columnas: "+rsmd.getColumnCount());
        
        for (int i=1; i<= nCol; i++)
        {
            
            System.out.printf("%-20s%-15s%-8s%-5s\n",
            rsmd.getColumnName(i),
            rsmd.getColumnTypeName(i),
            rsmd.isNullable(i),
            rsmd.getColumnDisplaySize(i));
        }
        
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet pk= dbmd.getPrimaryKeys(null, "ejemplo", tabla);
        StringBuilder claves= new StringBuilder();
        String sep="";
        while (pk.next())
        {
            claves.append(sep);
            claves.append(pk.getString(4));
            sep=" + ";
        }
        System.out.println("Clave primaria: "+ claves.toString());
        
        ResultSet fk= dbmd.getExportedKeys(null, "ejemplo", tabla);
        while (fk.next())
        {
            String fk_name=fk.getString("FKCOLUMN_NAME");
            String fk_tablename= fk.getString("FKTABLE_NAME");
            System.out.println("Tabla con Foreing Key: "+fk_tablename+",\nClave ajena: "+ fk_name);
        } 
        
        rs.close();
        pk.close();
    }

    private static void ejecutarDefinicion(Connection conn) throws SQLException 
    {
        Statement sentencia=conn.createStatement();
        
        System.out.println("¿Qué desea realizar?:\n"
                            +"1.Crear tabla.\n"
                            +"2.Borrar tabla.\n");
        int accion = Integer.parseInt(sc.nextLine());
        
        System.out.println("¿Qué tabla?:\n"
                            +"1.Empleados.\n"
                            +"2.Departamentos.\n");
        int tablaElegida = Integer.parseInt(sc.nextLine());
        
        if(accion == 1 )
        {
            String departamentos = "create table departamentos(dept_no tinyint primary key,dnombre varchar(10),loc varchar(10));";
            String empleados = "create table empleados(emp_no integer not null primary key,apellido varchar(10),oficio varchar(10),dir integer,\n" +
                               "fecha_alta DATE,salario real,comision real,dept_no tinyint,FOREIGN KEY(dept_no) REFERENCES departamentos(dept_no));";
            
            sentencia.executeUpdate((tablaElegida == 1)?empleados:departamentos);
        }
        if(accion == 2)
        {
            sentencia.executeUpdate("DROP TABLE "+((tablaElegida == 1)?"empleados":"departamentos")+";");           
        }
        
        sentencia.close();
    }

    private static void ejecutarManipulacion(Connection conn) throws SQLException, ClassNotFoundException
    {
        System.out.println("¿Qué desea realizar?:\n"
                         + "1. Insertar datos.\n"
                         + "2. Eliminar datos.\n"
                         + "3. Modificar datos. ");
        int opcion = Integer.parseInt(sc.nextLine());
        switch(opcion)
        {
            case 1:
                insertarDatos(conn);
                break;
            case 2:
                eliminarDatos(conn);
                break;
            case 3:
                modificarDatos(conn);
                break;
            default:
                System.out.println("Opción inválida");
                break;
        }      
    }
    private static void insertarDatos(Connection conn) throws SQLException, ClassNotFoundException 
    {
        System.out.println("¿En qué tabla quiere introducir datos?\n"
                         + "1.Departamento.\n"
                         + "2.Empleados.");
        int opcion = Integer.parseInt(sc.nextLine());
        
        Statement sentencia=conn.createStatement();
        int dept_no;
        String sql = "";
        int filas = 0;
        switch(opcion)
        {
            case 1:
                System.out.println("Introduzca el número de departamento:");
                dept_no = Integer.parseInt(sc.nextLine());
                System.out.println("Introduzca el nombre de departamento:");
                String dnombre = sc.nextLine();
                System.out.println("Introduzca la localización:");
                String loc = sc.nextLine();
                
                sql = "INSERT INTO departamentos VALUES("+dept_no+",'"+dnombre+"','"+loc+"');"; 
                filas = sentencia.executeUpdate(sql);
                break;
            case 2:
                System.out.println("Introduzca el número de empleado:(numérico)");
                int emp_no = Integer.parseInt(sc.nextLine());
                System.out.println("Introduzca el apellido:");
                String apellido = sc.nextLine();
                System.out.println("Introduzca el oficio:");
                String oficio = sc.nextLine();
                System.out.println("Introduzca el dir:(numérico)");
                int dir = Integer.parseInt(sc.nextLine());

                System.out.println("Introduzca el salario:(numérico)");
                double salario = Double.parseDouble(sc.nextLine());

                System.out.println("Introduzca el comision:(numérico)");
                double comision  = Double.parseDouble(sc.nextLine());
                System.out.println("Introduzca el número de departamento:(numérico)");
                dept_no = Integer.parseInt(sc.nextLine());
                        
                if(salario <  648.60){
                    System.out.println("El salario debe superar el SMI:(648,60)");
                    return;
                }
                
                if(existeEmpleado(emp_no))
                {
                    System.out.println("El empleado ya existe.");
                    return;
                }          
                if(!existeDepartamento(dept_no)){
                    System.out.println("El departamento no existe.");
                    return; 
                }
                    sql = "INSERT INTO empleados VALUES("+emp_no+",'"+apellido+"','"+oficio+"',"+dir+",now(),"+salario+","+comision+","+dept_no+");";  
                    filas = sentencia.executeUpdate(sql);
                break;           
                default:
                System.out.println("Opción elegida inválida.");
                break;   
        }       
        
        System.out.println("Filas modificadas: "+filas);
    }
    
    private static void eliminarDatos(Connection conn) throws SQLException, ClassNotFoundException
    {
        System.out.println("¿De qué tabla quiere eliminar datos?\n"
                         + "1.Departamento.\n"
                         + "2.Empleados.");
        int opcion = Integer.parseInt(sc.nextLine());
        Statement sentencia=conn.createStatement();
        int filas = 0;
        switch(opcion)
        {
            case 1:
                System.out.println("Introduzca el número de departamento.");
                int dept_no = Integer.parseInt(sc.nextLine());
                if(existeDepartamento(dept_no))
                {
                    filas = sentencia.executeUpdate("DELETE from departamentos where dept_no="+dept_no+";");
                    System.out.println("Filas eliminadas :"+filas);
                }else{
                    System.out.println("El departamento no existe.");
                }
                break;
            case 2:
                System.out.println("Introduzca el número de empleado.");
                int emp_no = Integer.parseInt(sc.nextLine());
                if(existeEmpleado(emp_no))
                {
                    filas = sentencia.executeUpdate("DELETE from empleados where emp_no="+emp_no+";");
                    System.out.println("Filas eliminadas :"+filas);
                }else{
                    System.out.println("El empleado no existe.");
                }                
                break;
            default:
                System.out.println("Opción elegida inválida.");
                break;
        }    
            
    }

    private static void modificarDatos(Connection conn) throws SQLException, ClassNotFoundException
    {
        System.out.println("¿De qué tabla quiere modificar un registro?\n"
                         + "1.Departamento.\n"
                         + "2.Empleados.");
        int opcion = Integer.parseInt(sc.nextLine());
        Statement sentencia=conn.createStatement();
        int filas = 0;
        switch(opcion)
        {
            case 1:
                System.out.println("Introduzca el nº de departamento.(numérico)");
                int dept_no = Integer.parseInt(sc.nextLine());
                if(existeDepartamento(dept_no))
                {
                    System.out.println("Introduzca el nuevo nombre del departamento.");
                    String dnombre = sc.nextLine();
                    System.out.println("Introduzca la nueva localización del departamento.");
                    String loc = sc.nextLine();
                    filas = sentencia.executeUpdate("UPDATE departamentos set dnombre='"+dnombre+"',loc='"+loc+"' where dept_no="+dept_no+";");
                    System.out.println("Filas modificadas: "+filas);
                }
                else
                {
                    System.out.println("El departamento indicado no existe.");
                }    
                break;
            case 2:
                System.out.println("Introduzca el nº de empleado.");
                int emp_no = Integer.parseInt(sc.nextLine());
                if(existeEmpleado(emp_no))
                {
                    System.out.println("Introduzca el apellido:");
                    String apellido = sc.nextLine();
                    System.out.println("Introduzca el oficio:");
                    String oficio = sc.nextLine();
                    System.out.println("Introduzca el dir:(numérico)");
                    int dir = Integer.parseInt(sc.nextLine());
                    System.out.println("Introduzca el salario:(numérico)");
                    double salario = Double.parseDouble(sc.nextLine());
                    System.out.println("Introduzca el comision:(numérico)");
                    double comision  = Double.parseDouble(sc.nextLine());
                    System.out.println("Introduzca el número de departamento:(numérico)");
                    dept_no = Integer.parseInt(sc.nextLine());
                        
                    if(salario <  648.60){
                        System.out.println("El salario debe superar el SMI:(648,60)");
                        break;
                    }
                    if(!existeDepartamento(dept_no)){
                       System.out.println("El número de departamento no existe.");
                        break; 
                    }
                    filas = sentencia.executeUpdate("UPDATE empleados set"
                                                     +" apellido='"+apellido
                                                     +"',oficio='"+oficio
                                                     +"',dir="+dir
                                                     +",salario="+salario
                                                     +",comision="+comision
                                                     +",dept_no="+ dept_no
                                                     +" where emp_no="+emp_no+";");
                    System.out.println("Filas modificadas: "+filas);
                    
                }
                else
                {
                    System.out.println("El empleado no existe.");
                }    
                break;
            default:
                System.out.println("Opción elegida inválida.");
                break;
        }
    }
    
    private static void ejecutarProcedimiento(Connection conn) throws SQLException
    {/*El procedimiento creado en MySQL workbench:
        CREATE DEFINER=`ejemplo`@`%` PROCEDURE `aumentoSueldo`(
	in numeroDepartamento int,
	in porcentaje float)
        BEGIN
                UPDATE empleados SET salario=salario+(salario*(porcentaje/100)) WHERE dept_no=numeroDepartamento;
        END
        */
   

        System.out.println("¿De qué departamento quiere aumentar el sueldo?");
        int dept_no = Integer.parseInt(sc.nextLine());       
        System.out.println("¿Què porcentaje?(no introducir %)");
        int porcentaje = Integer.parseInt(sc.nextLine());
        
        String sql = "{call aumentoSueldo(?,?)}";
        CallableStatement llamada=conn.prepareCall(sql);
        llamada.setInt(1, dept_no);
        llamada.setInt(2, porcentaje);
        llamada.execute();
        llamada.close();
        
        System.out.println("El salario del departamento"+dept_no+"ha sido aumentado un"+porcentaje+".");
    }
    
    private static boolean existeEmpleado(int emp_no) throws ClassNotFoundException, SQLException            
    {
        Connection conn = crearConexion();
        Statement sentencia = conn.createStatement();
        ResultSet r = sentencia.executeQuery("SELECT * from empleados where emp_no="+emp_no+";");
        return r.next();                   
    }
        
    private static boolean existeDepartamento(int dept_no) throws ClassNotFoundException, SQLException
    {
        Connection conn = crearConexion();
        Statement sentencia = conn.createStatement();
        ResultSet r = sentencia.executeQuery("SELECT * from departamentos where dept_no="+dept_no+";");
        return r.next();  
    }
    
   
    
}    
 
