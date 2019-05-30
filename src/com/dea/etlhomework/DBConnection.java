package com.dea.etlhomework;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	private final String URL = "jdbc:mysql://localhost:3306/"; // Ubicación de la BD.
    private  String BD = ""; // Nombre de la BD.
    private final String USER = "root";
    private final String PASSWORD = "dea111092";

    public Connection conexion = null;

    
    public DBConnection(int base) {
    	switch (base) {
		case 1:
			BD = "sakila";
			break;
		case 2:
			BD = "sakilaETL";
			break;
		default:
			break;
		}
    }
    
    @SuppressWarnings("finally")
    public Connection conectar() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conexion = DriverManager.getConnection(URL + BD, USER, PASSWORD);
            if (conexion != null) {
                System.out.println("¡Conexión Exitosa!");
            }
        } catch (Exception e) {
        	System.out.println("Error! Failed to connect: " + e.getMessage());
        } finally {
            return conexion;
        }
    }
}