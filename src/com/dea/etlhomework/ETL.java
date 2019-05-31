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
		    while (rs.next()) {
		    	//System.out.println(rs.getString("first_name"));
		    	try {
					String queryETL = "INSERT INTO actor (actor_id, name) VALUES(?, ?)";
					PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
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
			while(rs.next()) {
				try {
					String queryETL = "INSERT INTO store (store_id) VALUES(?)";
					PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
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
			while(rs.next()) {
				try {
					String queryETL = "INSERT INTO category (category_id, name) VALUES (?,?)";
					PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
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
			while(rs.next()) {
				try {
					String queryETL = "INSERT INTO film (film_id, title) VALUES(?,?)";
					PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
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
	
	public void ETLPayment() {
		String query = "SELECT payment.payment_id, film.film_id, staff.store_id, film_category.category_id, film_actor.actor_id, rental.rental_id, rental.inventory_id, amount, payment_date from payment join staff on payment.staff_id = staff.staff_id join rental on payment.rental_id = rental.rental_id join inventory on rental.inventory_id = inventory.inventory_id join film on inventory.film_id = film.film_id join film_category on film.film_id = film_category.category_id join film_actor on film.film_id = film_actor.film_id";
		DBConnection bd = new DBConnection(1);
		
		try {
			Statement sentencia = bd.conectar().createStatement();
			ResultSet rs = sentencia.executeQuery(query);
			DBConnection bdinsert = new DBConnection(2);
			while(rs.next()) {
				try {
					String queryETL = "INSERT INTO payment (film_id, store_id, category_id, actor_id, amount, payment_date) VALUES(?,?,?,?,?,?)";
					PreparedStatement preparedStmt = bdinsert.conectar().prepareStatement(queryETL);
					preparedStmt.setInt(1, Integer.parseInt(rs.getString("film_id")));
					preparedStmt.setInt(2, Integer.parseInt(rs.getString("store_id")));
					preparedStmt.setInt(3, Integer.parseInt(rs.getString("category_id")));
					preparedStmt.setInt(4, Integer.parseInt(rs.getString("actor_id")));
					preparedStmt.setFloat(5, Float.parseFloat(rs.getString("amount")));
					preparedStmt.setString(6, rs.getString("payment_date"));
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
}
