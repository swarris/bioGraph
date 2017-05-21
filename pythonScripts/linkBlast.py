import sys
from neo4j.v1 import GraphDatabase, basic_auth

# server settings
hostname = "phi-host.ab.wurnet.nl"
username = "neo4j"
password = "Neo4J"

# connect to database
driver = GraphDatabase.driver("bolt://{}".format(hostname), auth=basic_auth(username, password))
session = driver.session()

# process blast results
for l in open(sys.argv[1]):
    if l[0] != '#':
		# not a comment
        l = l.strip().split("\t")
		# get sequences IDs and similarity value
        id1 = l[0]
        id2 = l[1]
        sim = float(l[2])
		# connect genes based on their IDs
        session.run("match (a:gene_f_poae),(b:gene_f_gram) where a.geneID = '{}' and b.geneID = '{}' create (a)-[r:similarTo {{ identity: {} }}]->(b); ".format(id1, id2, sim))
