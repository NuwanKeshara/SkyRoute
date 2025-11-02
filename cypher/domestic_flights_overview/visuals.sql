MATCH (ap1:Airport)-[r:ROUTE]-(ap2:Airport) 
WHERE ap1.country = $neodash_airport_country OR ap2.country =  $neodash_airport_country 
RETURN count(DISTINCT r.airline) AS TotalAirlinesInUS


MATCH (a:Airport) 
WHERE a.country = $neodash_airport_country 
RETURN count(DISTINCT a) AS TotalAirports


MATCH (a1:Airport)-[r:ROUTE]->(a2:Airport) 
WHERE a1.country = $neodash_airport_country AND a2.country = $neodash_airport_country 
RETURN count(r) AS TotalFlights


MATCH (a1:Airport)-[r:ROUTE]->(a2:Airport) 
WHERE a1.country = $neodash_airport_country AND a2.country = $neodash_airport_country
RETURN a1,a2 LIMIT 200


MATCH (a1:Airport)-[r:ROUTE]-(a2:Airport) 
WHERE a1.country = $neodash_airport_country AND a2.country = $neodash_airport_country 
RETURN a1, r, a2 LIMIT 200


MATCH (a1:Airport)-[r:ROUTE]-(a2:Airport) 
WHERE a1.country = $neodash_airport_country and a2.country = $neodash_airport_country
RETURN a1.name + ' (' + a1.iata + ')' AS Airport_Name,
a1.city AS City,
count(r) AS Connected_Routes 
ORDER BY Connected_Routes DESC LIMIT 10


MATCH (a1:Airport)-[r:ROUTE]->(a2:Airport) 
WHERE a1.country = $neodash_airport_country AND a2.country = $neodash_airport_country 
RETURN a1.name AS Airport, count(r) AS OutgoingFlights 
ORDER BY OutgoingFlights DESC LIMIT 10


MATCH (a1:Airport)-[r:ROUTE]->(a2:Airport)
WHERE a1.country = $neodash_airport_country 
  AND a2.country = $neodash_airport_country
WITH a2.name AS Airline, count(r) AS FlightCount
ORDER BY FlightCount DESC
WITH collect({airline: Airline, flights: FlightCount}) AS data
UNWIND data AS d
WITH d, 
     CASE 
       WHEN d IN data[0..7] THEN d.airline
       ELSE 'Other'
     END AS AirlineGroup,
     d.flights AS FlightCount
RETURN AirlineGroup AS Airline, sum(FlightCount) AS FlightCount
ORDER BY FlightCount DESC;


MATCH (ap1:Airport)-[r:ROUTE]->(ap2:Airport)
WHERE ap1.country = $neodash_airport_country 
  AND ap2.country = $neodash_airport_country
WITH r.airline AS airlineIata, count(r) AS routeCount
MATCH (a:Airline {iata: airlineIata})
RETURN a.name AS Airline, routeCount
ORDER BY routeCount DESC
LIMIT 10;



