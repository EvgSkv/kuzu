MATCH (a:ND)-[e1:links]->(b:ND)-[e2:links]->(c:ND), (a)-[e3:links]->(d:ND)-[e4:links]->(c), (a)-[e5:links]->(c), (b)-[e6:links]->(d) WHERE a.X < 1000000 RETURN MIN(a.X), MIN(b.X), MIN(c.X), MIN(d.X)