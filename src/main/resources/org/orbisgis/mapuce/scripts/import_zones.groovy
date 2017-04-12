package org.orbisgis.mapuce.scripts

import org.orbiswps.groovyapi.input.*
import org.orbiswps.groovyapi.output.*
import org.orbiswps.groovyapi.process.*


/**
 * This process is used to import the table that contains the list of communes
 * from the MApUCE project.
 * The user has to specify (mandatory):
 *  - A login and a password to the remote database
 *
 * @return a table that contains the list of the communes that could be processed.
 *
 * @author Erwan Bocher
 */
@Process(title = "1-Import commune areas",
        description = "Import the commune areas table. <br> This table lists all communes for which the USR, buildings and roads data have been prepared.<br> The commune areas table is stored into a remote database. Please contact info@orbigis.org to obtain an account.",
        keywords = ["Vector","MAPuCE"])
def processing() {
if(!login.isEmpty()&& !password.isEmpty()){
    sql.execute "SET MAX_MEMORY_ROWS 100";
    def schemaFromRemoteDB = "lienss"
    def tableFromRemoteDB = "(SELECT CODE_INSEE, unite_urbaine, ogc_fid as id_zone FROM lienss.zone_etude)"	
    sql.execute "DROP TABLE IF EXISTS COMMUNES_TEMP,COMMUNES_MAPUCE "
    def query = "CREATE  LINKED TABLE COMMUNES_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    
    sql.execute query
    sql.execute "CREATE TABLE COMMUNES_MAPUCE AS SELECT * FROM COMMUNES_TEMP"
    sql.execute "CREATE INDEX ON COMMUNES_MAPUCE (CODE_INSEE)"
    sql.execute "CREATE INDEX ON COMMUNES_MAPUCE (id_zone)"
    sql.execute "DROP TABLE IF EXISTS COMMUNES_TEMP"
    literalOutput = "The commune areas have been imported."
}

}


/** Login to the MApUCE database. */
@LiteralDataInput(
        title="Login to the database",
        description="Login to the database")
String login 

/** Password to the MApUCE database. */
@PasswordInput(
        title="Password to the database",
        description="Password to the database")
String password 


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        description="The output message")
String literalOutput

