// Step 1: Direct paths (1-hop)
MATCH (a1:Airport {country: $neodash_airport_country_2})-[r1:ROUTE]->(a2:Airport {country: $neodash_airport_country_3})
WITH {path:[a1,a2], rels:[r1], sig:a1.iata + '->' + a2.iata} AS p
WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
LIMIT 5
WITH collect(p) AS directPaths

// Step 2: If no direct paths, try 2-hop
CALL apoc.do.when(
    size(directPaths) = 0,
    '
    MATCH (a1:Airport {country: $origin})-[r1:ROUTE]->(mid:Airport)-[r2:ROUTE]->(a2:Airport {country: $destination})
    WHERE mid.country <> $origin AND mid.country <> $destination
    WITH {path:[a1,mid,a2], rels:[r1,r2], sig:a1.iata + "->" + mid.iata + "->" + a2.iata} AS p
    WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
    LIMIT 5
    RETURN collect(p) AS paths
    ',
    'UNWIND $directPaths AS p RETURN collect(p) AS paths',
    {origin:$neodash_airport_country_2, destination:$neodash_airport_country_3, directPaths:directPaths}
) YIELD value
WITH value.paths AS twoHopOrDirectPaths

// Step 3: If no 1- or 2-hop paths, try 3-hop
CALL apoc.do.when(
    size(twoHopOrDirectPaths) = 0,
    '
    MATCH (a1:Airport {country: $origin})-[r1:ROUTE]->(mid1:Airport)-[r2:ROUTE]->(mid2:Airport)-[r3:ROUTE]->(a2:Airport {country: $destination})
    WHERE ALL(c IN [mid1.country, mid2.country] WHERE c <> $origin AND c <> $destination)
    WITH {path:[a1,mid1,mid2,a2], rels:[r1,r2,r3], sig:a1.iata + "->" + mid1.iata + "->" + mid2.iata + "->" + a2.iata} AS p
    WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
    LIMIT 5
    RETURN collect(p) AS paths
    ',
    'UNWIND $twoHopOrDirectPaths AS p RETURN collect(p) AS paths',
    {origin:$neodash_airport_country_2, destination:$neodash_airport_country_3, twoHopOrDirectPaths:twoHopOrDirectPaths}
) YIELD value
WITH value.paths AS finalPaths

// Step 4: Unwind for map markers
UNWIND finalPaths AS p
UNWIND p.path AS airport
UNWIND p.rels AS route
RETURN airport, route






// Step 1: Direct paths (1-hop)
MATCH (a1:Airport {country: $neodash_airport_country_2})-[r1:ROUTE]->(a2:Airport {country: $neodash_airport_country_3})
WITH {path:[a1,a2], rels:[r1], sig:a1.iata + '->' + a2.iata} AS p
WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
LIMIT 5
WITH collect(p) AS directPaths

// Step 2: If no direct paths, try 2-hop
CALL apoc.do.when(
    size(directPaths) = 0,
    '
    MATCH (a1:Airport {country: $origin})-[r1:ROUTE]->(mid:Airport)-[r2:ROUTE]->(a2:Airport {country: $destination})
    WHERE mid.country <> $origin AND mid.country <> $destination
    WITH {path:[a1,mid,a2], rels:[r1,r2], sig:a1.iata + "->" + mid.iata + "->" + a2.iata} AS p
    WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
    LIMIT 5
    RETURN collect(p) AS paths
    ',
    'UNWIND $directPaths AS p RETURN collect(p) AS paths',
    {origin:$neodash_airport_country_2, destination:$neodash_airport_country_3, directPaths:directPaths}
) YIELD value
WITH value.paths AS twoHopOrDirectPaths

// Step 3: If no 1- or 2-hop paths, try 3-hop
CALL apoc.do.when(
    size(twoHopOrDirectPaths) = 0,
    '
    MATCH (a1:Airport {country: $origin})-[r1:ROUTE]->(mid1:Airport)-[r2:ROUTE]->(mid2:Airport)-[r3:ROUTE]->(a2:Airport {country: $destination})
    WHERE ALL(c IN [mid1.country, mid2.country] WHERE c <> $origin AND c <> $destination)
    WITH {path:[a1,mid1,mid2,a2], rels:[r1,r2,r3], sig:a1.iata + "->" + mid1.iata + "->" + mid2.iata + "->" + a2.iata} AS p
    WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
    LIMIT 5
    RETURN collect(p) AS paths
    ',
    'UNWIND $twoHopOrDirectPaths AS p RETURN collect(p) AS paths',
    {origin:$neodash_airport_country_2, destination:$neodash_airport_country_3, twoHopOrDirectPaths:twoHopOrDirectPaths}
) YIELD value
WITH value.paths AS finalPaths

// Step 4: Unwind paths and relationships for graph view
UNWIND finalPaths AS p
UNWIND p.path AS airport
UNWIND p.rels AS route

RETURN DISTINCT airport AS Node, route AS Relationship







// Step 1: Direct paths (1-hop)
MATCH (a1:Airport {country: $neodash_airport_country_2})-[r1:ROUTE]-(a2:Airport {country: $neodash_airport_country_3})
WITH {path:[a1,a2], sig:a1.iata + '->' + a2.iata} AS p
WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
LIMIT 5
WITH collect(p) AS directPaths

// Step 2: If no direct paths, try 2-hop
CALL apoc.do.when(
    size(directPaths) = 0,
    '
    MATCH (a1:Airport {country: $origin})-[r1:ROUTE]-(mid:Airport)-[r2:ROUTE]-(a2:Airport {country: $destination})
    WHERE mid.country <> $origin AND mid.country <> $destination
    WITH {path:[a1,mid,a2], sig:a1.iata + "->" + mid.iata + "->" + a2.iata} AS p
    WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
    LIMIT 5
    RETURN collect(p) AS paths
    ',
    'UNWIND $directPaths AS p RETURN collect(p) AS paths',
    {origin:$neodash_airport_country_2, destination:$neodash_airport_country_3, directPaths:directPaths}
) YIELD value
WITH value.paths AS twoHopOrDirectPaths

// Step 3: If no 1- or 2-hop paths, try 3-hop
CALL apoc.do.when(
    size(twoHopOrDirectPaths) = 0,
    '
    MATCH (a1:Airport {country: $origin})-[r1:ROUTE]-(mid1:Airport)-[r2:ROUTE]-(mid2:Airport)-[r3:ROUTE]-(a2:Airport {country: $destination})
    WHERE ALL(c IN [mid1.country, mid2.country] WHERE c <> $origin AND c <> $destination)
    WITH {path:[a1,mid1,mid2,a2], sig:a1.iata + "->" + mid1.iata + "->" + mid2.iata + "->" + a2.iata} AS p
    WITH DISTINCT p.sig AS sig, head(collect(p)) AS p
    LIMIT 5
    RETURN collect(p) AS paths
    ',
    'UNWIND $twoHopOrDirectPaths AS p RETURN collect(p) AS paths',
    {origin:$neodash_airport_country_2, destination:$neodash_airport_country_3, twoHopOrDirectPaths:twoHopOrDirectPaths}
) YIELD value
WITH value.paths AS finalPaths

// Step 4: Convert each path to readable string for table
UNWIND finalPaths AS p
RETURN REDUCE(s = "", airport IN p.path |
    s + CASE WHEN s = "" THEN "" ELSE "  ----  " END + airport.country + " (" + airport.iata + ")"
) AS Paths
ORDER BY Paths
