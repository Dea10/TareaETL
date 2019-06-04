package com.dea.etlhomework;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.util.ArrayList;

public class ETL {

	//ArrayList<String> extract = new ArrayList<>();
	
	public void ETLActor() {
		String query = "SELECT actor_id, first_name, last_name FROM actor";
		DBConnection bd = new DBConnection(1);
		
		try {
		    Statement sentencia = bd.conectar().createStatement();
		    ResultSet rs = sentencia.executeQuery(query);
		    DBConnection bdinsert = new DBConnection(2);
		    
		    String queryETL = "INSERT INTO actor (actor_id, name) VALUES(?, ?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
		    while (rs.next()) {
		    	try {
			        preparedStmt.setInt(1, Integer.parseInt(rs.getString("actor_id")));
			        preparedStmt.setString(2, rs.getString("first_name")  + " " + rs.getString("last_name"));
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
	
	public void ETLStore() {
		String query = "SELECT store_id FROM store";
		DBConnection bd = new DBConnection(1);
		
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			
			String queryETL = "INSERT INTO store (store_id) VALUES(?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
			while(rs.next()) {
				try {
					preparedStmt.setInt(1, Integer.parseInt(rs.getString("store_id")));
					preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO store failed: " + e.getMessage());
				}
			}
			sentencia.close();
			bd.conexion.close();
			bdinsert.conexion.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void ETLCategory() {
		String query = "SELECT category_id, name FROM category";
		DBConnection bd = new DBConnection(1);
		
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			
			String queryETL = "INSERT INTO category (category_id, name) VALUES (?,?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
			while(rs.next()) {
				try {
					preparedStmt.setInt(1, Integer.parseInt(rs.getString("category_id")));
					preparedStmt.setString(2, rs.getString("name"));
					preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO category failed: " + e.getMessage());
				}
			}
			sentencia.close();
			bd.conexion.close();
			bdinsert.conexion.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void ETLFilm() {
		String query = "SELECT film_id, title FROM film";
		DBConnection bd = new DBConnection(1);
		
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			
			String queryETL = "INSERT INTO film (film_id, title) VALUES(?,?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
			while(rs.next()) {
				try {
					preparedStmt.setInt(1, Integer.parseInt(rs.getString("film_id")));
					preparedStmt.setString(2, rs.getString("title"));
					preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO film failed: " + e.getMessage());
				}
			}
			sentencia.close();
			bd.conexion.close();
			bdinsert.conexion.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void ETLDate() {
		String query = "SELECT DISTINCT DATE(payment_date) as \"dmy\", DATE_FORMAT(payment_date, \"%d\") AS \"day\", DATE_FORMAT(payment_date, \"%m\") AS \"month\", DATE_FORMAT(payment_date, \"%Y\") AS \"year\" FROM payment";
		DBConnection bd = new DBConnection(1);
		
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			
			String queryETL = "INSERT INTO payment_date (dmy, day, month, year) VALUES(?,?,?,?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
			while(rs.next()) {
				try {
					preparedStmt.setString(1, rs.getString("dmy"));
					preparedStmt.setInt(2, rs.getInt("day"));
					preparedStmt.setInt(3, rs.getInt("month"));
					preparedStmt.setInt(4, rs.getInt("year"));
					preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO payment_date failed: " + e.getMessage());
				}
			}
			sentencia.close();
			bd.conexion.close();
			bdinsert.conexion.close();
		} catch (Exception e) {
			System.out.println("payment_date handled exception: " + e.getMessage());
		}
	}
	
	public void ETLPayment() {
		String query = "SELECT "
				+ "payment.payment_id, "
				+ "film.film_id, "
				+ "staff.store_id, "
				+ "film_category.category_id, "
				+ "film_actor.actor_id, "
				+ "rental.rental_id, "
				+ "rental.inventory_id, "
				+ "amount, "
				+ "payment_date AS \"dmy\", "
				+ "DATE_FORMAT(payment_date, \"%d\") AS \"day\", "
				+ "DATE_FORMAT(payment_date, \"%m\") AS \"month\", "
				+ "DATE_FORMAT(payment_date, \"%Y\") AS \"year\" "
				+ "FROM payment "
				+ "JOIN staff ON payment.staff_id = staff.staff_id "
				+ "JOIN rental ON payment.rental_id = rental.rental_id "
				+ "JOIN inventory ON rental.inventory_id = inventory.inventory_id "
				+ "JOIN film ON inventory.film_id = film.film_id "
				+ "JOIN film_category ON film.film_id = film_category.category_id "
				+ "JOIN film_actor ON film.film_id = film_actor.film_id";
		DBConnection bd = new DBConnection(1);
		
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			
			String queryETL = "INSERT INTO payment (film_id, store_id, category_id, actor_id, amount, payment_date, day, month, year) VALUES(?,?,?,?,?,?,?,?,?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
			while(rs.next()) {
				try {
					preparedStmt.setInt(1, Integer.parseInt(rs.getString("film_id")));
					preparedStmt.setInt(2, Integer.parseInt(rs.getString("store_id")));
					preparedStmt.setInt(3, Integer.parseInt(rs.getString("category_id")));
					preparedStmt.setInt(4, Integer.parseInt(rs.getString("actor_id")));
					preparedStmt.setFloat(5, Float.parseFloat(rs.getString("amount")));
					preparedStmt.setString(6, rs.getString("dmy"));
					preparedStmt.setInt(7, Integer.parseInt(rs.getString("day")));
					preparedStmt.setInt(8, Integer.parseInt(rs.getString("month")));
					preparedStmt.setInt(9, Integer.parseInt(rs.getString("year")));
					preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO payment failed: " + e.getMessage());
				}
			}
			sentencia.close();
			bd.conexion.close();
			bdinsert.conexion.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void dataSetPayment() {
		String query = "SELECT payment.payment_id AS payment_id, " + 
				"film.title AS film_title, " + 
				"staff.store_id AS store, " + 
				"category.name AS category_name, " + 
				"actor.first_name AS actor_first_name, " + 
				"actor.last_name AS actor_last_name, " + 
				"amount, " + 
				"payment_date, " + 
				"DATE_FORMAT(payment_date, \"%d\") AS \"day\", " +
				"DATE_FORMAT(payment_date, \"%m\") AS \"month\", " +
				"DATE_FORMAT(payment_date, \"%Y\") AS \"year\" " +
				"FROM payment " + 
				"JOIN staff ON payment.staff_id = staff.staff_id " + 
				"JOIN rental ON payment.rental_id = rental.rental_id " + 
				"JOIN inventory ON rental.inventory_id = inventory.inventory_id " + 
				"JOIN film ON inventory.film_id = film.film_id " + 
				"JOIN film_category ON film.film_id = film_category.film_id " + 
				"JOIN category ON film_category.category_id = category.category_id " + 
				"JOIN film_actor ON film.film_id = film_actor.film_id " + 
				"JOIN actor ON film_actor.actor_id = actor.actor_id;";
		DBConnection bd = new DBConnection(1);
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			
			String queryETL = "INSERT INTO payment_dataset "
					+ "(payment_id, "
					+ "film_title, "
					+ "store, "
					+ "category, "
					+ "actor, "
					+ "amount, "
					+ "payment_date, "
					+ "day, "
					+ "month, "
					+ "year) "
					+ "VALUES(?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
			
			while(rs.next()) {
				System.out.print(".");
				try {
					preparedStmt.setInt(1, rs.getInt("payment_id"));
					preparedStmt.setString(2, rs.getString("film_title"));
					preparedStmt.setInt(3, rs.getInt("store"));
					preparedStmt.setString(4, rs.getString("category_name"));
					preparedStmt.setString(5, rs.getString("actor_first_name") + rs.getString("actor_last_name"));
					preparedStmt.setFloat(6, Float.parseFloat(rs.getString("amount")));
					preparedStmt.setString(7, rs.getString("payment_date"));
					preparedStmt.setInt(8, rs.getInt("day"));
					preparedStmt.setInt(9, rs.getInt("month"));
					preparedStmt.setInt(10, rs.getInt("year"));
					preparedStmt.execute();
				} catch (Exception e) {
					System.out.println("Error! Query INSERT INTO payment_dataset failed: " + e.getMessage());
				}
			}
			sentencia.close();
			bd.conexion.close();
			bdinsert.conexion.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
