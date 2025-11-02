MATCH (a1:Airport)-[r:ROUTE]-(a2:Airport) 
WHERE a1.country = $neodash_airport_country_1  AND a2.country <> $neodash_airport_country_1
RETURN count(DISTINCT a2.country) AS TotalInternationalVisits


MATCH (a1:Airport)-[r:ROUTE]-(a2:Airport) 
WHERE a1.country = $neodash_airport_country_1  AND a2.country <> $neodash_airport_country_1
RETURN count(DISTINCT a1.name) AS TotalInternationalVisits


MATCH (us_airport:Airport)-[r:ROUTE]->(int_airport:Airport) 
WHERE us_airport.country = $neodash_airport_country_1  AND int_airport.country <> $neodash_airport_country_1
RETURN count(DISTINCT r) AS TotalInternationalRoutes


MATCH (a1:Airport)-[r:ROUTE]->(a2:Airport)
WHERE a1.country = $neodash_airport_country 
  AND a2.country <> $neodash_airport_country
WITH a2.country AS Country, count(r) AS Count
ORDER BY Count DESC
WITH collect({airline: Country, flights: Count}) AS data
UNWIND data AS d
WITH d, 
     CASE 
       WHEN d IN data[0..7] THEN d.airline
       ELSE 'Other'
     END AS AirlineGroup,
     d.flights AS CountryCount
RETURN AirlineGroup AS cCount, sum(CountryCount) AS CountryCount
ORDER BY CountryCount DESC;


MATCH (a1:Airport)-[r:ROUTE]->(a2:Airport) 
WHERE a1.country = $neodash_airport_country_1 AND a2.country <> $neodash_airport_country_1
RETURN a2


MATCH (us_airport:Airport)-[r:ROUTE]-(int_airport:Airport) 
WHERE us_airport.country = $neodash_airport_country_1 AND int_airport.country <> $neodash_airport_country_1
RETURN us_airport, r, int_airport LIMIT 100


MATCH (src:Airport {country: $neodash_airport_country_1})-[r:ROUTE]->(dst:Airport)
WHERE dst.country <> $neodash_airport_country_1
MATCH (al:Airline {iata: r.airline})
RETURN al.name AS Airline_Name, al.country AS Airline_Country, count(r) AS International_Routes
ORDER BY International_Routes DESC
LIMIT 20;



