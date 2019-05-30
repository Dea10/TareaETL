package com.dea.etlhomework;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class ETL {

	ArrayList<String> extract = new ArrayList<>();
	
	public void ETLActors() {
		String query = "SELECT actor_id, first_name, last_name FROM actor";
		DBConnection bd = new DBConnection(1);
		
		try {
		    Statement sentencia = bd.conectar().createStatement();
		    ResultSet rs = sentencia.executeQuery(query);
		    DBConnection bdinsert = new DBConnection(2);
		    while (rs.next()) {
		    	//System.out.println(rs.getString("first_name"));
		    	try {
					String queryETL = "INSERT INTO actor (actor_id, name) VALUES(?, ?)";
					PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			        preparedStmt.setInt(1, Integer.parseInt(rs.getString("actor_id")));
			        preparedStmt.setString(2, rs.getString("first_name") + rs.getString("last_name"));
			        preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO actor failed: " + e.getMessage());
				}
		    }
		    sentencia.close();
		    bd.conexion.close();
		    bdinsert.conexion.close();
		} catch (SQLException e) {
		    System.out.println(e.getMessage());
		}
		
	}
	
}
