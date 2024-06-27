# Bernard de Bono
from pymysql import connect
from neo4j import GraphDatabase
import re
from collections import defaultdict


class n4jAct:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_matNode(self, matID, ontoID, nm):
        with self.driver.session() as session:
            matNode = session.write_transaction(self._createMatNode_and_return, matID, ontoID, nm)
            print(matNode)

    def print_matRel(self, matParentID, matConstituentID):
        with self.driver.session() as session:
            print(matParentID+' gej '+matConstituentID)
            matREL = session.write_transaction(self._createMatRel_and_return, matParentID, matConstituentID)
            print(matREL)

    def print_lyphNode(self, lyphID):
        with self.driver.session() as session:
            lyphNode = session.write_transaction(self._createLyphNode_and_return, lyphID)
            print(lyphNode)

    def print_matRelLyph(self, matID, lyphID):
        with self.driver.session() as session:
            matRelLyph = session.write_transaction(self._createMatRelLyph_and_return, matID, lyphID)
            print(matRelLyph)

    def print_TOOMapComponentBioNode(self, bioID):
        with self.driver.session() as session:
            TOOMapComponentBioNode = session.write_transaction(self._createTOOMapComponentBioNode_and_return, bioID)
            print(TOOMapComponentBioNode)

    def print_TOOMapComponentGeoNode(self, geoID):
        with self.driver.session() as session:
            TOOMapComponentGeoNode = session.write_transaction(self._createTOOMapComponentGeoNode_and_return, geoID)
            print(TOOMapComponentGeoNode)

    def print_TOOMapComponentNode(self, cID, cNAME):
        with self.driver.session() as session:
            TOOMapComponentNode = session.write_transaction(self._createTOOMapComponentNode_and_return, cID, cNAME)
            print(TOOMapComponentNode)

    def print_TOOMapComponentRels(self, bID, gID, cID):
        with self.driver.session() as session:
            TOOMapComponentRels = session.write_transaction(self._createTOOMapComponentRels_and_return, bID, gID, cID)
            print(TOOMapComponentRels)

    def print_AddToTOOMapAnchorGeoNode(self, ID, nm, onto):
        with self.driver.session() as session:
            AddToTOOMapAnchorGeoNode = session.write_transaction(self._AddToTOOMapAnchorGeoNode_and_return, ID, nm, onto)
            print(AddToTOOMapAnchorGeoNode)

    def print_TOOMapWireRelAnchor(self, nWIRE, nANCHOR):
        with self.driver.session() as session:
            print(nWIRE, nANCHOR)
            TOOMapWireRelAnchor = session.write_transaction(self._createTOOMapWireRelAnchor_and_return, nWIRE, nANCHOR)
            print(TOOMapWireRelAnchor)

    def print_createChainNodeAndRelToWire(self, cChain, cNAME, cWIRE):
        with self.driver.session() as session:
            print(cChain, 'is', cNAME)
            createChainNodeAndRelToWire = session.write_transaction(self._createChainNodeAndRelToWire_and_return, cChain, cNAME, cWIRE)
            print(createChainNodeAndRelToWire)

    def print_createConcatenationOfLyphsForWire(self, wChain, wLyphSequence):
        with self.driver.session() as session:
            print(wChain, wLyphSequence)
            ConcatenationOfLyphsForWire = session.write_transaction(self._createConcatenationOfLyphsForWire_and_return, wChain, wLyphSequence)
            print(ConcatenationOfLyphsForWire)

    def print_createLyphTemplates(self, templateID, templateLabel, templateOntologyTerm, templateTopology):
        with self.driver.session() as session:
            LyphTemplates = session.write_transaction(self._createLyphTemplates_and_return, templateID, templateLabel, templateOntologyTerm, templateTopology)
            print(LyphTemplates)

    def print_createLyphs(self, lyphID, lyphLabel, lyphOntologyTerm, lyphTopology):
        with self.driver.session() as session:
            Lyphs = session.write_transaction(self._createLyphs_and_return, lyphID, lyphLabel, lyphOntologyTerm, lyphTopology)
            print(Lyphs)

    @staticmethod
    def _createMatNode_and_return(tx, mID, mONTO, mNM):
        result = tx.run("MERGE (a:MATERIAL {name: $mID, ontologyTerm: $mONTO, label: $mNM})"
                        "RETURN a.name + ', from node ' + id(a)", mID=mID, mONTO=mONTO, mNM=mNM)
        return result.single()[0]

    @staticmethod
    def _createMatRel_and_return(tx, mpID, mcID):
        result = tx.run("MATCH (p:MATERIAL {name: $mpID})"
                        "MATCH (c:MATERIAL {name: $mcID})"
                        "MERGE (p)-[:HAS_MATERIAL_CONSTITUENT]->(c)"
                        "RETURN p.name + ', has constituent ' + c.name", mpID=mpID, mcID=mcID)
        return result.single()[0]

    @staticmethod
    def _createLyphNode_and_return(tx, lID):
        result = tx.run("MERGE (a:LYPH {name: $lID})"
                        "RETURN a.name + ', from node ' + id(a)", lID=lID)
        return result.single()[0]

    @staticmethod
    def _createMatRelLyph_and_return(tx, mpID, lcID):
        result = tx.run("MATCH (p:MATERIAL {name: $mpID})"
                        "MATCH (c:LYPH {name: $lcID})"
                        "MERGE (p)-[:HAS_LYPH_CONSTITUENT]->(c)"
                        "RETURN p.name + ', has constituent ' + c.name", mpID=mpID, lcID=lcID)
        return result.single()[0]

    @staticmethod
    def _createTOOMapComponentBioNode_and_return(tx, bID):
        result = tx.run("MERGE (a:TOOMAPBIOCLASS {name: $bID})"
                        "RETURN a.name + ', from node ' + id(a)", bID=bID)
        return result.single()[0]

    @staticmethod
    def _createTOOMapComponentGeoNode_and_return(tx, gID):
        result = tx.run("MERGE (a:TOOMAPGEOCLASS {name: $gID})"
                        "RETURN a.name + ', from node ' + id(a)", gID=gID)
        return result.single()[0]

    @staticmethod
    def _createTOOMapComponentNode_and_return(tx, cID, cNAME):
        result = tx.run("MERGE (a:TOOMAPCOMPONENT {name: $cID, label: $cNAME})"
                        "RETURN a.name + ', from node ' + id(a)", cID=cID, cNAME=cNAME)
        return result.single()[0]

    @staticmethod
    def _createTOOMapComponentRels_and_return(tx, bID, gID, cID):
        result = tx.run("MATCH (b:TOOMAPBIOCLASS {name: $bID})"
                        "MATCH (g:TOOMAPGEOCLASS {name: $gID})"
                        "MATCH (c:TOOMAPCOMPONENT {name: $cID})"
                        "MERGE (b)<-[:IS_OF_TOOMAP_BIOCLASS]-(c)"
                        "MERGE (g)<-[:IS_OF_TOOMAP_GEOCLASS]-(c)"
                        "RETURN c.name + ', bioclass ' + b.name", bID=bID, gID=gID, cID=cID)
        return result.single()[0]

    @staticmethod
    def _AddToTOOMapAnchorGeoNode_and_return(tx, ID, nm, onto):
        result = tx.run("MATCH (a:TOOMAPCOMPONENT {name: $ID}) "
                        "SET a = {name: $ID, label: $nm, ontologyTerm: $onto}"
                        "RETURN a.name", ID=ID, nm=nm, onto=onto)
        return result.single()[0]

    @staticmethod
    def _createTOOMapWireRelAnchor_and_return(tx, nWIRE, nANCHOR):
        print(nWIRE, '--', nANCHOR)
        result = tx.run("MATCH (w:TOOMAPCOMPONENT {name: $nWIRE}) "
                        "MATCH (a:TOOMAPCOMPONENT {name: $nANCHOR})"
                        "MERGE (w)-[:WIRE_HAS_ANCHOR]->(a)"
                        "RETURN a.name", nWIRE=nWIRE, nANCHOR=nANCHOR)
        return result.single()[0]

    @staticmethod
    def _createChainNodeAndRelToWire_and_return(tx, cChain, cNAME, cWIRE):
        print(cChain, '--', cNAME)
        dealString = "MERGE (c:WBKGCHAIN {name: \'"+cChain+"\', label: \'"+cNAME+"\'}) \n"
        if cWIRE != "":
            dealString = "MATCH (w:TOOMAPCOMPONENT {name: \'"+cWIRE+"\'}) \n"+dealString+"MERGE (c)-[:CHAIN_IS_LINKED_TO_WIRE]->(w) \n"

        dealString = dealString + "RETURN c.name"
        print(dealString)
        if cWIRE != "":
            print("skip")
            result = tx.run(dealString, cChain=cChain, cNAME=cNAME, cWIRE=cWIRE)
        else:
            result = tx.run(dealString, cChain=cChain, cNAME=cNAME)
        return result.single()[0]

    @staticmethod
    def _createConcatenationOfLyphsForWire_and_return(tx, wChain, wLyphSequence):
        print(wLyphSequence, '--', wLyphSequence)
        sequenceRow = re.split(r', ', wLyphSequence.strip())
        dlString = "MATCH (c:WBKGCHAIN {name: \'"+wChain+"\'}) \n"
        sourceLyph = 'c'
        targetLyph = ''
        targetLyphCount = 0
        for chainLyph in sequenceRow:
            print(chainLyph)
            targetLyphCount += 1
            targetLyph = 'l' + str(targetLyphCount)
            dlString = dlString + "MERGE ("+targetLyph+":LYPH {name: \'"+chainLyph+"\'}) \nMERGE ("+sourceLyph+")-[:NEXT_LYPH_IN_CHAIN]->("+targetLyph+") \n"
            sourceLyph = targetLyph
        dlString = dlString + "RETURN c.name"
        print(dlString)
        print(' = = = = = ')
        result = tx.run(dlString, wChain=wChain, wLyphSequence=wLyphSequence)
        return result.single()[0]

    @staticmethod
    def _createLyphTemplates_and_return(tx, templateID, templateLabel, templateOntologyTerm, templateTopology):
        if templateOntologyTerm:
            onto = templateOntologyTerm
        else:
            onto = ""
        if templateTopology:
            topo = templateTopology
        else:
            topo = ""
        result = tx.run("MERGE (t:LYPHTEMPLATE {name: $templateID, label: $templateLabel, ontologyTerm: $onto, topology: $topo})"
                        "RETURN t.name + ', label ' + t.label", templateID=templateID, templateLabel=templateLabel, onto=onto, topo=topo)
        return result.single()[0]

    @staticmethod
    def _createLyphs_and_return(tx, lyphID, lyphLabel, lyphOntologyTerm, lyphTopology):
        if lyphOntologyTerm:
            lonto = lyphOntologyTerm
        else:
            lonto = "undef"
        if lyphTopology:
            ltopo = lyphTopology
        else:
            ltopo = "undef"
        print("LYPHS AT LAST:", lyphID, lyphLabel, lonto, ltopo)
        result = tx.run("MATCH (l:LYPH) "
                        "WHERE l.name=$lyphID "
                        "SET l+= {label:$lyphLabel, ontologyTerm:$lonto, topology:$ltopo} "
                        "WITH count(l) as nodesAltered "
                        "WHERE nodesAltered = 0 "
                        "CREATE (l:LYPH {name: $lyphID, label: $lyphLabel, ontologyTerm: $lonto, topology: $ltopo}) "
                        "RETURN l.name", lyphID=lyphID, lyphLabel=lyphLabel, lonto=lonto, ltopo=ltopo)
        res = result.single()
        if res and len(res) > 0:
            return res[0]
        else:
            return "No lyph was affected"

###########################
# Path settings and MySQL & Neo4j connectors
###########################
# rootdir='/Users/bdb/Documents/neo4j/WBKG/origWBKGcsv/'
rootdir='data/origWBKGcsv/'
origMaterialsFile='matWBKG.csv'
origTOOMapComponentsFile='TOOMapComponents.csv'
origTOOMapAnchorsFile='TOOMapAnchors.csv'
origTOOMapWiresFile='TOOMapWires.csv'
origTOOMapRegionsFile='TOOMapRegions.csv'
origWBKGChainsFile='WBKGchains.csv'
origWBKGLyphsFile='WBKGLyphs.csv'

import os
neo4j_address = os.environ.get('APINATOMY_NEO4J')
neo4j_pwd = os.environ.get('APINATOMY_NEO4J_PASSWORD')
mysql_user = os.environ.get('APINATOMY_MYSQL_USER')
mysql_pwd = os.environ.get('APINATOMY_MYSQL_PASSWORD')

sqldata_base = connect(database="dbkrbjea5snysy", user=mysql_user, passwd=mysql_pwd, host="es35.siteground.eu", port=3306)
n4 = n4jAct(neo4j_address, "neo4j", neo4j_pwd)


sqlcur = sqldata_base.cursor()

###########################
# MYSQL
###########################
def importMaterialsCSVtoSQL():
    matSource = rootdir+origMaterialsFile
    with open(matSource) as file_object:
        for line in file_object:
            newLine = re.sub(r"'", "", line)
            checkForList = re.split(r'\"',newLine)
            fileRow = re.split(r',',newLine)
            if fileRow[0] != "":
                matQuery = "insert into materials values(NULL,\'"+fileRow[0].strip()+"\',\'"+fileRow[1].strip()+"\',\'"+fileRow[2].strip()+"\',\'"+fileRow[3].strip()+"\',\'"
                if len(checkForList) > 1:
                    matQuery = matQuery + checkForList[1].strip()+"\')"
                else:
                    matQuery = matQuery + fileRow[4].strip()+"\')"

                print(matQuery)
                sqlcur.execute(matQuery)
    sqldata_base.commit()


###########################
def importTOOMapComponentsCSVtoSQL():
    TOOCompSource = rootdir + origTOOMapComponentsFile
    with open(TOOCompSource) as file_object:
        for line in file_object:
            fileRow = re.split(r'\"', line)
            if fileRow[0] != "":
                classRow = re.split(r',', fileRow[0])
                details = re.split(r'-', classRow[0].strip())
                bioClass = details[1].strip()
                geoClass = details[0].strip()
                for id in re.split(r',', fileRow[1]):
                    TOOCompQuery = "insert into TOOMapComponents values(NULL,'"+bioClass.strip().upper()+"\',\'"+geoClass.strip().upper()+"\',\'"+id.strip()+"\')"
                    print(TOOCompQuery)
                    sqlcur.execute(TOOCompQuery)
                sqldata_base.commit()


###########################
def importTOOMapAnchorsCSVtoSQL():
    TOOCompAnchorsource = rootdir + origTOOMapAnchorsFile
    with open(TOOCompAnchorsource) as file_object:
        for line in file_object:
            fileRow = re.split(r',', line)
            if fileRow[0] != "":
                ID = fileRow[0].strip()
                name = fileRow[1].strip()
                hostedBy = fileRow[3].strip()
                ontologyTerm = fileRow[5].strip()
                TOOCompQuery = "insert into TOOMapAnchorGeoComponent values(NULL,'" + ID + "\',\'" + name + "\',\'" + hostedBy + "\',\'" + ontologyTerm + "\')"
                print(TOOCompQuery)
                sqlcur.execute(TOOCompQuery)
        sqldata_base.commit()


###########################
def importTOOMapWiresCSVtoSQL():
    TOOCompWiresource = rootdir + origTOOMapWiresFile
    with open(TOOCompWiresource) as file_object:
        for line in file_object:
            fileRow = re.split(r',', line)
            if fileRow[0] != "":
                ID = fileRow[0].strip()
                name = fileRow[1].strip()
                source = fileRow[2].strip()
                target = fileRow[3].strip()
                if fileRow[11] != "":
                    ontologyTerm = fileRow[11].strip()
                else:
                    ontologyTerm=""
                TOOCompQuery = "insert into TOOMapWireGeoComponent values(NULL,'" + ID + "\',\'" + name + "\',\'" + source  + "\',\'" + target + "\',\'" + ontologyTerm + "\')"
                print(TOOCompQuery)
                sqlcur.execute(TOOCompQuery)
        sqldata_base.commit()


###########################
def importTOOMapRegionsCSVtoSQL():
    TOOCompRegionsource = rootdir + origTOOMapRegionsFile
    with open(TOOCompRegionsource) as file_object:
        for line in file_object:
            fileRow = re.split(r',', line)
            if fileRow[0] != "":
                ID = fileRow[0].strip()
                name = fileRow[1].strip()
                quadrant = fileRow[5].strip()
                if fileRow[6] != "":
                    ontologyTerm = fileRow[6].strip()
                else:
                    ontologyTerm=""
                TOOCompQuery = "insert into TOOMapRegionGeoComponent values(NULL,'" + ID + "\',\'" + name + "\',\'" + quadrant  + "\',\'" + ontologyTerm + "\')"
                print(TOOCompQuery)
                sqlcur.execute(TOOCompQuery)
        sqldata_base.commit()


###########################
def importWBKGChainsCSVtoSQL():
    WBKGChainsource = rootdir + origWBKGChainsFile
    with open(WBKGChainsource) as file_object:
        for line in file_object:
            fileRow = re.split(r',', line)
            if fileRow[0] != "":
                chainID = fileRow[0].strip()
                chainName = fileRow[1].strip()

                if fileRow[2] != "":
                    chainWire = fileRow[2].strip()
                else:
                    chainWire=""

                if fileRow[3].find("\"") != -1:
                    del fileRow[0]
                    del fileRow[0]
                    del fileRow[0]
                    chainProper = '  '.join(fileRow).strip()
                    chainProper= re.sub(r"\"", "", chainProper.strip())
                    chainProper = re.sub(r"  ", ", ", chainProper.strip())
                else:
                    chainProper = fileRow[3].strip()

                TOOCompQuery = "insert into chains values(NULL,'" + chainID + "\',\'" + chainName + "\',\'" + chainProper + "\',\'" + chainWire + "\')"
                print(TOOCompQuery)
                sqlcur.execute(TOOCompQuery)
        sqldata_base.commit()


###########################
def importLyphsCSVtoSQL():
    WBKGLyphsource = rootdir + origWBKGLyphsFile
    with open(WBKGLyphsource) as file_object:
        for line in file_object:
            newLine =re.sub(r"\'", "", line.strip())
            fileRow = re.split(r',', newLine)
            if fileRow[0] != "":
                lyphID = fileRow[0].strip()
                ontologyTerm =  fileRow[1].strip()
                label= fileRow[2].strip()

                isTemplate = 0
                if fileRow[3].strip() != "" and fileRow[3].strip() == "TRUE":
                    isTemplate=1

                topology = fileRow[4].strip()
                supertype = fileRow[5].strip()
                hostedBy = fileRow[6].strip()
                del fileRow[0]
                del fileRow[0]
                del fileRow[0]
                del fileRow[0]
                del fileRow[0]
                del fileRow[0]
                del fileRow[0]

                remainingLine = ', '.join(fileRow)
                remainingLine = re.sub(r"\"", "", remainingLine.strip())
                layers = ''
                internalLyphs = ''
                internalLyphsInLayers = ''

                #print(remainingLine)
                listRow = re.split(r', end', remainingLine)
                #del listRow[0]
                layers = listRow[0].strip()
                if listRow[1].strip() != "":
                    internalLyphs = listRow[1].strip()
                if listRow[2].strip() != "":
                    internalLyphsInLayers = listRow[2].strip()

                TOOCompQuery = "insert into lyphs values(NULL,'" + lyphID + "\',\'" + ontologyTerm + "\',\'" + label + "\'," + str(isTemplate) + ",\'" + topology + "\',\'" + supertype+ "\',\'" + hostedBy + "\',\'" + layers + "\',\'" + internalLyphs + "\',\'" + internalLyphsInLayers + "\')"
                print(TOOCompQuery)
                sqlcur.execute(TOOCompQuery)
        sqldata_base.commit()

###########################
#NEO4J
###########################
def importMaterialsSQLtoGraph():
    cons = defaultdict(list)
    sqlMaterialQuery = "select id, ontologyTerms, name, materials from materials"
    sqlcur.execute(sqlMaterialQuery)
    tableMaterials = sqlcur.fetchall()

    for data in tableMaterials:
        print(data[0])
        n4.print_matNode(data[0].strip(), data[1].strip(), data[2].strip())
        if (data[3].strip() != ""):
            for constituent in re.split(r',\s*', data[3]):
                print(constituent)
                if 'lyph-cell-' in constituent:
                    n4.print_lyphNode(constituent)
                    n4.print_matRelLyph(data[0].strip(), constituent.strip())
                else:
                    cons[data[0].strip()].append(constituent.strip())

    for key in cons:
        for c in cons[key]:
            print(key + 'constituted by' + c)
            n4.print_matRel(key, c)

###########################
def importTOOMapComponentsSQLtoGraph():
    sqlTOOMapComponents = "select component_biological_class, component_geometric_class, component_id, name from TOOMapComponents"
    sqlcur.execute(sqlTOOMapComponents)
    tableComponents = sqlcur.fetchall()

    for data in tableComponents:
        print(data[0])
        n4.print_TOOMapComponentBioNode(data[0].strip().upper())
        print(data[1])
        n4.print_TOOMapComponentGeoNode(data[1].strip())
        print(data[2])
        n4.print_TOOMapComponentNode(data[2].strip(), data[3].strip())
        n4.print_TOOMapComponentRels(data[0].strip().upper(), data[1].strip(), data[2].strip())

###########################
def importTOOMapWiresSQLtoGraph():
    sqlTOOMapWires = "select distinct component_id from TOOMapComponents where component_geometric_class=\'wires\' AND component_id like \"w-%\" AND length(component_id)>3"
    sqlcur.execute(sqlTOOMapWires)
    tableWires = sqlcur.fetchall()

    for data in tableWires:
        wordRow = re.split(r'-', data[0][2:].strip())
        for anchor in wordRow:
            print(data[0], ' has', anchor)
            n4.print_TOOMapWireRelAnchor(data[0].strip(), anchor.strip())

###########################
def importWBKGOrderedChainsOfLyphsSQLtoGraph():
    sqlWBKGOrderedChainsOfLyphs = "select ID, label, wire_id, lyph_sequence from chains"
    sqlcur.execute(sqlWBKGOrderedChainsOfLyphs)
    tableChains = sqlcur.fetchall()

    for data in tableChains:
        if data[2].strip() != "":
            chainWire = data[2].strip()
        else:
            chainWire = ""
        n4.print_createChainNodeAndRelToWire(data[0].strip(), data[1].strip(), chainWire)
        n4.print_createConcatenationOfLyphsForWire(data[0].strip(), data[3].strip())

###########################
def importWBKGLyphAndTemplateNodes():
    # sqlimportWBKGTemplateNodes = "select ID, label, ontologyTerm, topology from lyphs where isTemplate"
    # sqlcur.execute(sqlimportWBKGTemplateNodes)
    # tableLyphTemplates = sqlcur.fetchall()
    #
    # for data in tableLyphTemplates:
    #     n4.print_createLyphTemplates(data[0].strip(), data[1].strip(), data[2].strip(), data[3].strip())

    sqlimportWBKGLyphs = "select ID, label, ontologyTerm, topology from lyphs where not isTemplate"
    sqlcur.execute(sqlimportWBKGLyphs)
    tableLyphs = sqlcur.fetchall()
    for data in tableLyphs:
        n4.print_createLyphs(data[0].strip(), data[1].strip(), data[2].strip(), data[3].strip())

###########################
#MAIN
###########################
#Importing CSVs into MySQL
###########################
#importMaterialsCSVtoSQL()
#importTOOMapComponentsCSVtoSQL()
#importTOOMapAnchorsCSVtoSQL()
#importTOOMapWiresCSVtoSQL()
#importTOOMapRegionsCSVtoSQL()
#importWBKGChainsCSVtoSQL()
#importLyphsCSVtoSQL()

###########################
#Convert knowledge in sql tables into neo4j graphs
###########################
# print("Step 1")
# importMaterialsSQLtoGraph()
# print("Step 2")
# importTOOMapComponentsSQLtoGraph()
# print("Step 3")
# importTOOMapWiresSQLtoGraph()
# print("Step 4")
# importWBKGOrderedChainsOfLyphsSQLtoGraph()
# print("Done")

importWBKGLyphAndTemplateNodes()
#importWBKGLyphToMaterialRels()

n4.close()
sqldata_base.close()
###########################
#END
###########################