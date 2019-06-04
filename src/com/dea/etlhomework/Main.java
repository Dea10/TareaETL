package com.dea.etlhomework;

public class Main {

	public static void main(String[] args) {
		
		ETL etl = new ETL();
		
		//etl.ETLActor();
		//etl.ETLStore();
		//etl.ETLCategory();
		//etl.ETLFilm();
		//etl.ETLDate();
		etl.ETLPayment();
		
		System.out.println("ETL finished!");
	}

}
