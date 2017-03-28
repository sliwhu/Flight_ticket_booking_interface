-- ==================
cd C:\Lisa's documents\hw6-514
path C:\Program Files\Java\jdk1.8.0_121\bin
set CLASSPATH=.;sqljdbc4.jar
javac -g FlightService.java Query.java
java FlightService
-- ---------------------------
Task  0
search "Seattle WA" "Boston MA" 1 14 10
search "Seattle WA" "Boston MA" 0 14 10

				// breakpoint
				// BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
				// System.out.print("> THIS IS MY BREAKPOINT!!!!");
				// String command = null;
				// command = r.readLine();



SELECT TOP (?) a.year AS year,a.month_id AS month_id,a.day_of_month AS day_of_month_a,a.carrier_id AS carrier_id_a,a.flight_num AS flight_num_a, a.origin_city AS origin_city_a, a.dest_city AS dest_city_a, b.day_of_month AS day_of_month_b,b.carrier_id AS carrier_id_b,b.flight_num AS flight_num_b, " 
b.origin_city AS origin_city_b, b.dest_city AS dest_city_b, (a.actual_time+b.actual_time) AS actual_time FROM Flights AS a JOIN Flights AS b ON a.dest_city=b.origin_city "
					WHERE a.origin_city = ? AND b.dest_city = ? AND a.day_of_month = ? AND a.year = 2015 AND a.month_id = 7 
					b.year = 2015 AND b.month_id = 7 
					AND a.actual_time IS NOT NULL AND b.actual_time IS NOT NULL 
					ORDER BY actual_time ASC 

SELECT TOP (10) a.year,a.month_id,a.day_of_month,a.carrier_id,a.flight_num,a.origin_city, (a.actual_time+b.actual_time ) AS actual_t FROM Flights AS a JOIN Flights AS b ON a.dest_city=b.origin_city WHERE a.origin_city = 'Seattle WA' AND b.dest_city = 'Boston MA' AND a.day_of_month = 14 AND a.year = 2015 AND a.month_id = 7 ORDER BY actual_t ASC;


Instruction

front tier: command line, website
logic tier: query.java, flightservice.java
data tier: microsoft sql server




"seattle" "boston" 1 (direct flight or not) 14 10 (top 10)

prepared statement: to avoid hacker using ? 
question mark is used to avoid sql injection

Demo:
search"Seattle WA" "Boston WA" 1 20 15

searching for flight
Itinerary #0: 1 flights(s), 314 minutes

1 means 1 hop here.


reservations:
researvation 6: origin="Las Vegas NV" Date-2015-7-3  Carrier-AA 

final: practice on previous finals.


--创建了customer和reservation表格 哪里使用？
--prepared statement?





