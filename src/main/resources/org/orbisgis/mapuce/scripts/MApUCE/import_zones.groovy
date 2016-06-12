import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*
import javax.swing.JOptionPane;


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
@Process(title = "Import study areas (Communes)",
        resume = "Import the study areas.\n A study area is a commune where the blocks are computed.",
        keywords = "Vector,MAPuCE")
def processing() {
if(!login.isEmpty()&& !password.isEmpty()){
    def schemaFromRemoteDB = "lienss"
    def tableFromRemoteDB = "(SELECT * FROM lienss.zone_etude)"	
    def query = "CREATE  LINKED TABLE COMMUNE_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute "DROP TABLE IF EXISTS COMMUNE_TEMP"
    sql.execute query
    sql.execute "CREATE TABLE COMMUNES_MAPUCE AS SELECT * FROM COMMUNE_TEMP"
    sql.execute "DROP TABLE IF EXISTS COMMUNE_TEMP"
    literalOutput = "The data commune has been imported"
}

}


/** Login to the MApUCE database. */
@LiteralDataInput(
        title="Login to the database",
        resume="Login to the database")
String login 

/** Password to the MApUCE database. */
@LiteralDataInput(
        title="Password to the database",
        resume="Password to the database")
String password 


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        resume="The output message")
String literalOutput

