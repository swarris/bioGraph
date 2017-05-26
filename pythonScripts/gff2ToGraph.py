import sys
from neo4j.v1 import GraphDatabase, basic_auth

# server settings
hostname = "localhost:7474"
username = "neo4j"
password = "Neo4J"

# connect to neo4j
driver = GraphDatabase.driver("bolt://{}".format(hostname), auth=basic_auth(username, password))
session = driver.session()

# filename with annotations in GFF format
GFF = open(sys.argv[1],"r")
# name of the organism / genome
organism = sys.argv[2]

# set containing the chromosome names in the GFF
sequences = set()
# set of gene IDs
geneIDs = set()
# admin while reading IDs
prevID = ""

def createRNA(gene):
    '''
    Link coding part (CDS) from single gene together to form RNA.
    This function adds the relationship 'mRNA'.
    '''
    # find the CDS of this gene and sort the CDS based on start locations in the genome
    node = session.run("match (a:CDS) where a.geneID = '{}' return a.start as start order by a.start;".format(gene))
    prevStart = 0
    # step through all CDS
    for n in node:
        if prevStart > 0:
            # Get CDS based on starting positions and link them with 'mRNA'
            session.run("match (a:CDS),(b:CDS) where a.geneID = '{}' and b.geneID = '{}' and a.start = {} and b.start = {} create unique (a)-[r:mRNA]->(b); ".format(gene, gene, prevStart, n["start"]))
        prevStart = int(n["start"])


def createGeneStructure(gene):
    '''
    Create gene structure by linking CDS and introns together.
    This function adds the relationship 'links'.
    '''

    # find the CDS and introns of this gene and sort based on start locations in the genome
    node = session.run("match (a) where (a:CDS or a:intron) and a.geneID = '{}' return a.start as start order by a.start;".format(gene))
    prevStart = 0;
    for n in node:
        if prevStart > 0:
            # connect elements of this gene
            session.run("match (a),(b) where (a:CDS or a:intron)  and (b:CDS or b:intron) and a.geneID = '{}' and b.geneID = '{}' and a.start = {} and b.start = {} create unique (a)-[r:links]->(b); ".format(gene, gene, prevStart, n["start"]))
        prevStart = int(n["start"])


def createSequenceStructure():
    '''
    Gets all sequences from the database and connects the genes on each sequence in order.
    This function adds the relationship 'order'.
    '''
    global organism
    # get all sequences from the database
    seq = session.run("match (a:sequence) return a.name as name;")
    for s in seq:
        prevStart = 0
        # get all genes in this sequence and order them by starting point
        node = session.run("match (a:gene_{}) where a.sequence = '{}' return a.start as start order by a.start;".format(organism,s["name"]))
        for n in node:
            if prevStart > 0:
                # link genes
                session.run("match (a:gene_{}),(b:gene_{}) where a.sequence = '{}' and b.sequence = '{}' and a.start = {} and b.start = {} create unique (a)-[r:order]-(b); ".format(organism, organism, s["name"], s["name"], prevStart, n["start"]))
            prevStart = int(n["start"])

        
# step through the GFF, line by line
for l in GFF:
    # columns are separated by tabs
    l = l.split("\t")
    if len(l)>= 9:
        # get annotation info from file
        chr = l[0]
        node = l[2]
        start = l[3]
        end = l[4]
        geneID = l[8].strip()
        # check for gene_id in geneID:
        if "gene_id" in geneID:
            ids = geneID.split(";")
            for g in ids:
                if "gene_id" in g:
                    geneID = g.split("\"")[1]
        geneID = geneID.split(".")[0]
        # geneID not already processed?
        if geneID not in geneIDs and prevID != "":
            createRNA(prevID)
            createGeneStructure(prevID)
        prevID = geneID

        geneIDs.add(geneID)
        # add only specific elements to database:
        if node in ["gene", "transcript", "CDS", "intron"]:
            if node == "gene":
                node = "gene_" + organism
            session.run("create (n:{} {{sequence: '{}', start: {}, end: {}, geneID: '{}'}});".format(node, chr, start, end, geneID))
            sequences.add(chr)
        #print("create (n:{} {{chromosome: '{}', start: {}, end: {}, geneID: '{}'}});".format(node, chr, start, end, geneID))

createRNA(prevID)
createGeneStructure(prevID)

for s in sequences:
    session.run("create (n:sequence_{} {{name: '{}', organism: '{}'}});".format(organism, s, organism))
    #print("create (n:sequence {{name: '{}'}});".format(s))

createSequenceStructure()

#link genes to sequence:
session.run("match (a:sequence_{}),(b:gene_{}) where a.name = b.sequence create (b)-[r:on]->(a); ".format(organism, organism))
#print("match (a:sequence),(b:gene) where a.name = b.sequence create (b)-[r:on]->(a); ")
#link transcripts to genes:
session.run("match (a:gene_{}),(b:transcript) where a.geneID = b.geneID create (b)-[r:transcribes]->(a); ".format(organism))
#link CDS to transcripts:
session.run("match (a:transcript),(b:CDS) where a.geneID = b.geneID create (b)-[r:codes]->(a); ")
#link intron to transcripts:
session.run("match (a:transcript),(b:intron) where a.geneID = b.geneID create (b)-[r:in]->(a); ")




        
