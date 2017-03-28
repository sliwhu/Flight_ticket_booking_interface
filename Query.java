import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Runs queries against a back-end database
 */
public class Query {

	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;

	// DB Connection
	private Connection conn;

	// Logged In User
	private String username;
	private int cid=-1;	 // Unique customer ID
	

	class Flight {
		int fid_a;
		int fid_b;
		int year;
		int monthId;
		int dayOfMonth_a;
		int dayOfMonth_b;
		String carrierId_a;
		String carrierId_b;
		String flightNum_a;
		String flightNum_b;
		String originCity_a;
		String originCity_b;
		String destCity_a;
		String destCity_b;
		int time;
	}

    private List<Flight> last_result; 

	// Canned queries
	private static final String LOGIN_SQL =
			"SELECT cid, password FROM Customers WHERE username = ?" ;
	private PreparedStatement loginStatement;

       // search (one hop) -- This query ignores the month and year entirely. You can change it to fix the month and year
       // to July 2015 or you can add month and year as extra, optional, arguments
	private static final String SEARCH_ONE_HOP_SQL =
			"SELECT TOP (?) fid, year,month_id,day_of_month,carrier_id,flight_num,origin_city, dest_city, actual_time "
					+ "FROM Flights "
					+ "WHERE origin_city = ? AND dest_city = ? AND day_of_month = ? AND year = 2015 AND month_id = 7 AND actual_time IS NOT NULL "
					+ "ORDER BY actual_time ASC ";
					
					
	private PreparedStatement searchOneHopStatement;


	private static final String SEARCH_TWO_HOP_SQL =
			"SELECT TOP (?) a.fid AS fid_a, b.fid AS fid_b, a.year AS year,a.month_id AS month_id,a.day_of_month AS day_of_month_a,a.carrier_id AS carrier_id_a,a.flight_num AS flight_num_a, "
					+ "a.origin_city AS origin_city_a, a.dest_city AS dest_city_a, "
					+ "b.day_of_month AS day_of_month_b, b.carrier_id AS carrier_id_b, b.flight_num AS flight_num_b, " 
					+ "b.origin_city AS origin_city_b, b.dest_city AS dest_city_b, (a.actual_time+b.actual_time) AS actual_time "
					+ "FROM Flights AS a JOIN Flights AS b ON (a.dest_city=b.origin_city AND a.day_of_month = b.day_of_month) "
					+ "WHERE a.origin_city = ? AND b.dest_city = ? AND a.day_of_month = ? AND a.year = 2015 AND a.month_id = 7 "
					+ "AND b.year = 2015 AND b.month_id = 7 "
					+ "AND a.actual_time IS NOT NULL AND b.actual_time IS NOT NULL "
					+ "ORDER BY actual_time ASC ";


	private PreparedStatement searchTwoHopStatement;

	private static final String CURRENTBOOKCOUNT_SQL =
			"SELECT bookcount FROM Capacity WHERE fid = ?";
		private PreparedStatement currentbookcountStatement;

	private static final String UPDATECAPACITY1_SQL =
		"UPDATE CAPACITY SET bookcount=bookcount+1 WHERE fid = ?";
	private PreparedStatement updatecapacity1Statement;

	private static final String UPDATECAPACITY2_SQL =
		"UPDATE CAPACITY SET bookcount=bookcount-1 WHERE fid = ?";
	private PreparedStatement updatecapacity2Statement;

	private static final String HAVESAMEDATE_SQL =
	"SELECT count(*) AS num FROM Reservations WHERE cid = ? AND date=?";
	private PreparedStatement havesamedateStatement;

	private static final String TRANSACTION_BOOK_SQL =
			"INSERT INTO Reservations (cid, fid_a, fid_b, date) VALUES(?, ?, ?, ?)" ;
	private PreparedStatement bookStatement;			

       // TODO: Add more queries here
	private static final String RESERVATION_SQL =
			"SELECT R.rid, R.cid, R.fid_a, R.fid_b "
					+ "FROM Reservations AS R "
					+ "WHERE cid = ?";
	private PreparedStatement reservationStatement;	


	private static final String PRINTFLIGHT_SQL = 
			"SELECT fid, year, month_id, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time "
			+ "FROM Flights "
			+ "WHERE fid = ?";
	private PreparedStatement printflightStatement;

	private static final String CHECK_SQL =
			" SELECT rid, fid_a, fid_b FROM Reservations WHERE rid=? AND cid=?";
	private PreparedStatement checkStatement;

	private static final String CANCEL_SQL = 
			"DELETE FROM Reservations WHERE rid= ? AND cid= ?" ;
	private PreparedStatement cancelStatement;

	private static final String DUMMY_UPDATE =
			"UPDATE Reservations SET fid_b=fid_b WHERE rid = ?";
	private PreparedStatement dummyupdateStatement;


	// transactions
	private static final String BEGIN_TRANSACTION_SQL =  
			"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;"; 
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;


	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

	/**********************************************************/
	/* Connection code to SQL Azure.  */
	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("flightservice.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("flightservice.url");
		jSQLUser	   = configProps.getProperty("flightservice.sqlazure_username");
		jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
		conn = DriverManager.getConnection(jSQLUrl, // database
				jSQLUser, // user
				jSQLPassword); // password

		conn.setAutoCommit(true); //by default automatically commit after each statement 

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

	}

	public void closeConnection() throws Exception {
		conn.close();
	}

	/**********************************************************/
	/* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */


	public void prepareStatements() throws Exception {
		searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
		loginStatement = conn.prepareStatement(LOGIN_SQL);
 		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
		bookStatement = conn.prepareStatement(TRANSACTION_BOOK_SQL);
		reservationStatement = conn.prepareStatement(RESERVATION_SQL);
		cancelStatement = conn.prepareStatement(CANCEL_SQL);
		searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP_SQL);
		currentbookcountStatement = conn.prepareStatement (CURRENTBOOKCOUNT_SQL);
		havesamedateStatement = conn.prepareStatement (HAVESAMEDATE_SQL);
		updatecapacity1Statement = conn.prepareStatement(UPDATECAPACITY1_SQL);
		updatecapacity2Statement = conn.prepareStatement(UPDATECAPACITY2_SQL);
		printflightStatement = conn.prepareStatement(PRINTFLIGHT_SQL);
		checkStatement = conn.prepareStatement(CHECK_SQL);
		dummyupdateStatement = conn.prepareStatement(DUMMY_UPDATE);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
	}
	
	public void transaction_login(String username, String password) throws Exception {
		loginStatement.clearParameters();
		loginStatement.setString(1, username);

		ResultSet LoginResults = loginStatement.executeQuery();
		if (LoginResults.next()) {

					String result_password = LoginResults.getString("password");
					int result_cid = LoginResults.getInt("cid");
					cid=result_cid;
					if (result_password.equals(password)) {
						System.out.println("Welcome, " + username);

					}
					else {
						System.out.println("Wrong password, please try again.");
					}


					
  		}else{
  			System.out.println("Username doesn't exist");
  		}
		LoginResults.close();
	}

	/**
	 * Searches for flights from the given origin city to the given destination
	 * city, on the given day of the month. If "directFlight" is true, it only
	 * searches for direct flights, otherwise is searches for direct flights
	 * and flights with two "hops". Only searches for up to the number of
	 * itineraries given.
	 * Prints the results found by the search.
	 */
	public void transaction_search_safe(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws Exception {
		last_result = new ArrayList<Flight>();
		int flag=0, row=0;
		ResultSet oneHopResults;
		try{
			searchOneHopStatement.clearParameters();
			searchOneHopStatement.setInt(1, numberOfItineraries);
			searchOneHopStatement.setString(2, originCity);
			searchOneHopStatement.setString(3, destinationCity);
			searchOneHopStatement.setInt(4, dayOfMonth);
			oneHopResults = searchOneHopStatement.executeQuery();	
			
		}catch (SQLException e) {
			System.out.println ("Please enter a valid input.");
			System.out.println(e);
			return;
		}

		while (oneHopResults.next()) {
					Flight f = new Flight();
					f.fid_a=oneHopResults.getInt("fid");
					f.year = oneHopResults.getInt("year");
					f.monthId = oneHopResults.getInt("month_id");
					f.dayOfMonth_a = oneHopResults.getInt("day_of_month");
					f.carrierId_a = oneHopResults.getString("carrier_id");
					f.flightNum_a = oneHopResults.getString("flight_num");
					f.originCity_a = oneHopResults.getString("origin_city");
					f.destCity_a = oneHopResults.getString("dest_city");
					f.time = oneHopResults.getInt("actual_time");
					last_result.add(f);
					System.out.println("ItineraryId: " + row + ", Number_of_stops: " + 0 + ", Fid: " + f.fid_a + ", Year: " + f.year + ", Month: " + f.monthId + ", Day: " + f.dayOfMonth_a + ", CarrierId: " + f.carrierId_a + ", FlightNum: " + f.flightNum_a + ", Origincity: " + f.originCity_a + ", Destcity: " + f.destCity_a + ", Time: " + f.time);
					row++;
					flag++;
  		}

		oneHopResults.close();
		
		if (!directFlight && row<numberOfItineraries){// if 2 hops are ok
				int numberOfItineraries_twohop=numberOfItineraries-row;
				searchTwoHopStatement.clearParameters();
				searchTwoHopStatement.setInt(1, numberOfItineraries_twohop);
				searchTwoHopStatement.setString(2, originCity);
				searchTwoHopStatement.setString(3, destinationCity);
				searchTwoHopStatement.setInt(4, dayOfMonth);
				ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
				while (twoHopResults.next()) {
						Flight f = new Flight();
						f.fid_a=twoHopResults.getInt("fid_a");
						f.fid_b=twoHopResults.getInt("fid_b");
						f.year = twoHopResults.getInt("year");
						f.monthId = twoHopResults.getInt("month_id");
						f.dayOfMonth_a = twoHopResults.getInt("day_of_month_a");
						f.dayOfMonth_b = twoHopResults.getInt("day_of_month_b");
						f.carrierId_a = twoHopResults.getString("carrier_id_a");
						f.carrierId_b = twoHopResults.getString("carrier_id_b");
						f.flightNum_a = twoHopResults.getString("flight_num_a");
						f.flightNum_a = twoHopResults.getString("flight_num_a");
						f.originCity_a = twoHopResults.getString("origin_city_a");
						f.originCity_b = twoHopResults.getString("origin_city_b");
						f.destCity_a = twoHopResults.getString("dest_city_a");
						f.destCity_b = twoHopResults.getString("dest_city_b");
						f.time = twoHopResults.getInt("actual_time");
						last_result.add(f);
						System.out.println("ItineraryId: " + row + ", Number_of_stops: " + 1 + ", Fid_a: " + f.fid_a + ", Year: " + f.year + ", Month: " + f.monthId + ", Day_a: " + f.dayOfMonth_a + ", CarrierId_a: " + f.carrierId_a + ", FlightNum_a: " + f.flightNum_a + ", Origincity_a: " + f.originCity_a + ", Destcity_a: " + f.destCity_a + ", Fid_b: " + f.fid_b + ", Year: " + f.year + ", Month: " + f.monthId + ", Day_b: " + f.dayOfMonth_b + ", CarrierId_b: " + f.carrierId_b + ", FlightNum_b: " + f.flightNum_b + ", Origincity_b: " + f.originCity_b + ", Destcity_b: " + f.destCity_b + ", Time: " + f.time);						
						row++;
						flag++;
	  			}

	  		if(flag==0){
	  			System.out.println ("No matched results.");
	  		}
		twoHopResults.close();
		}
	}
					
 
	
	public void transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) throws Exception{

            // one hop itineraries
            String unsafeSearchSQL =
                "SELECT TOP (" + numberOfItineraries +  ") year,month_id,day_of_month,carrier_id,flight_num,origin_city,actual_time "
                + "FROM Flights "
                + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity +  "\' AND day_of_month =  " + dayOfMonth + " "
                + "ORDER BY actual_time ASC";

            System.out.println("Submitting query: " + unsafeSearchSQL);
            Statement searchStatement = conn.createStatement();
            ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

            while (oneHopResults.next()) {
                int result_year = oneHopResults.getInt("year");
                int result_monthId = oneHopResults.getInt("month_id");
                int result_dayOfMonth = oneHopResults.getInt("day_of_month");
                String result_carrierId = oneHopResults.getString("carrier_id");
                String result_flightNum = oneHopResults.getString("flight_num");
                String result_originCity = oneHopResults.getString("origin_city");
                int result_time = oneHopResults.getInt("actual_time");
                System.out.println("Flight: " + result_year + "," + result_monthId + "," + result_dayOfMonth + "," + result_carrierId + "," + result_flightNum + "," + result_originCity + "," + result_time);
            }
            oneHopResults.close();
        }


	public void transaction_book(int itineraryId) throws Exception {

		int result_bookcount1=-1, result_bookcount2=-1, bookedtimes=-1;
		if (cid==-1 ) {
			System.out.println("You have to login before booking any flights");
		}else if(last_result==null){
			System.out.println("You have to search before booking any flights");
		}else if (last_result.size()==0){
			System.out.println("You have to search with valid results before booking any flights");
		}
		else if(itineraryId>last_result.size()){
			System.out.println("You have to enter a valid itineraryId from your search result.");
		}
		else {
			Flight b = last_result.get(itineraryId);

			beginTransaction();
			//write reservation
			bookStatement.clearParameters();
			bookStatement.setInt(1, cid);
			bookStatement.setInt(2, b.fid_a);
			bookStatement.setInt(3, b.fid_b);
			bookStatement.setInt(4, b.dayOfMonth_a);
			
			bookStatement.executeUpdate();
			//update capacity
			updatecapacity1Statement.clearParameters();
			updatecapacity1Statement.setInt(1, b.fid_a);
			updatecapacity1Statement.executeUpdate();
			updatecapacity1Statement.clearParameters();
			updatecapacity1Statement.setInt(1, b.fid_b);
			
			updatecapacity1Statement.executeUpdate();
			
			//check constraints of capacity 3 for each flight
			currentbookcountStatement.clearParameters();
			currentbookcountStatement.setInt(1, b.fid_a);
			
			ResultSet currentbookcountResults1 = currentbookcountStatement.executeQuery();
			if(currentbookcountResults1.next()){
				result_bookcount1 = currentbookcountResults1.getInt("bookcount");
				currentbookcountResults1.close();
			}
			currentbookcountStatement.clearParameters();
			currentbookcountStatement.setInt(1, b.fid_b);
			
			ResultSet currentbookcountResults2 = currentbookcountStatement.executeQuery();
			if(currentbookcountResults2.next()){
				result_bookcount2 = currentbookcountResults2.getInt("bookcount");	
				currentbookcountResults2.close();
			}
		
			havesamedateStatement.clearParameters();
			havesamedateStatement.setInt(1, cid);
			havesamedateStatement.setInt(2, b.dayOfMonth_a);
	    	ResultSet havesamedaterResults = havesamedateStatement.executeQuery();
			if (havesamedaterResults.next()) {
				bookedtimes = havesamedaterResults.getInt("num");
				
			}
			
			
			if (result_bookcount1 > 3 || result_bookcount2 >3) {
				System.out.println("The flight(s) has reached maximum capacity, please book other flights.");
				rollbackTransaction();
			}else if (bookedtimes>1) {
				System.out.println("Book failed, you already booked a flight on this date.");
								//breakpoint
				// BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
				// System.out.print("> THIS IS MY BREAKPOINT!!!!");
				// String command = null;
				// command = r.readLine();
				rollbackTransaction();
			}else{
				System.out.println("Congratulations! You have successfully booked the following flight: ");
				this.print_flight(b.fid_a);
				this.print_flight(b.fid_b);
				// BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
				// System.out.print("> THIS IS MY BREAKPOINT!!!!");
				// String command = null;
				// command = r.readLine();
				commitTransaction();
			}
	

		}
		 
  		
	}

	public void print_flight(int fid) throws Exception{
		printflightStatement.clearParameters();
		printflightStatement.setInt(1, fid);
		ResultSet printflightResults = printflightStatement.executeQuery();
		
		if (printflightResults.next()) {

				int result_fid = printflightResults.getInt("fid");
				int result_year = printflightResults.getInt("year");
				int result_monthId = printflightResults.getInt("month_id");
				int result_dayOfMonth = printflightResults.getInt("day_of_month");
				String result_carrierId = printflightResults.getString("carrier_id");
				String result_flightNum = printflightResults.getString("flight_num");
				String result_originCity = printflightResults.getString("origin_city");
				String result_destCity = printflightResults.getString("dest_city");
				int result_time = printflightResults.getInt("actual_time");
				System.out.println("Fid: " + result_fid + ", Year:" + result_year + ", Month:" + result_monthId + ", Day:" + result_dayOfMonth + ", CarrierId" + result_carrierId + ", Flightnum:" + result_flightNum + ", Origincity:" + result_originCity + ", Destcity:" + result_destCity + ", Time:" + result_time);				
		}
	}

	public void transaction_reservations() throws Exception {
		if(cid==-1){
			System.out.println("You have to login to view your reservations.");
			return;
		}
		reservationStatement.clearParameters();
		reservationStatement.setInt(1, cid);
		ResultSet reservationResults = reservationStatement.executeQuery();
		if(reservationResults==null){
			System.out.println("Currently, you don't have any reservations.");
		}
		while (reservationResults.next()) {
				int fid_a= reservationResults.getInt("fid_a");
				int fid_b= reservationResults.getInt("fid_b");
				int rid=reservationResults.getInt("rid");
				System.out.println("ReservationID: "+rid+": ");
				this.print_flight(fid_a);
				this.print_flight(fid_b);
		}
		reservationResults.close();
	}

	public void transaction_cancel(int reservationId) throws Exception {
		int fid_a=-1, fid_b=-1;
		if (cid==-1) {
			System.out.println("You have to login before cancel any flights");
		}
		else {
			beginTransaction();

			dummyupdateStatement.clearParameters();
			dummyupdateStatement.setInt(1, reservationId);
			//block the process here if there is another transaction happening
			dummyupdateStatement.executeUpdate();

			checkStatement.clearParameters();
			checkStatement.setInt(1, reservationId);
			checkStatement.setInt(2, cid);
			ResultSet checkResults =  checkStatement.executeQuery();
			while (checkResults.next()){
				fid_a=checkResults.getInt("fid_a");
				fid_b=checkResults.getInt("fid_b");
			}
			if(fid_a==-1){
				System.out.println("You have entered an invalid reservationId");
				rollbackTransaction();
			}else{
				cancelStatement.clearParameters();
				cancelStatement.setInt(1, reservationId);
				cancelStatement.setInt(2, cid);
				cancelStatement.executeUpdate();
				System.out.println("You have successfulled cancelled reservation: " + reservationId);
				//update capacity table
				updatecapacity2Statement.clearParameters();
				updatecapacity2Statement.setInt(1, fid_a);
				updatecapacity2Statement.executeUpdate();
				updatecapacity2Statement.clearParameters();
				updatecapacity2Statement.setInt(1, fid_b);
				updatecapacity2Statement.executeUpdate();
				//breakpoint
				// BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
				// System.out.print("> THIS IS MY BREAKPOINT!!!!");
				// String command = null;
				// command = r.readLine();	
				commitTransaction();
			}


		}
		
	}

   public void beginTransaction() throws Exception {
        conn.setAutoCommit(false);
        beginTransactionStatement.executeUpdate();  
    }

    public void commitTransaction() throws Exception {
        commitTransactionStatement.executeUpdate(); 
        conn.setAutoCommit(true);
    }
    public void rollbackTransaction() throws Exception {
        rollbackTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
        } 
//use this function to test queries
    public void checksql(String query) throws Exception{
    	String unsafeSearchSQL = query;

            System.out.println("Submitting query: " + unsafeSearchSQL);
            Statement searchStatement = conn.createStatement();
            ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

            ResultSetMetaData rsmd = oneHopResults.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while (oneHopResults.next()) {
			    for (int i = 1; i <= columnsNumber; i++) {
			        if (i > 1) System.out.print(",  ");
			        String columnValue = oneHopResults.getString(i);
			        System.out.print(columnValue + " " + rsmd.getColumnName(i));
			    }
			    System.out.println("");
			}
            oneHopResults.close();
    }

}
