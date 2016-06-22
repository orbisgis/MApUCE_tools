import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*
import javax.swing.JOptionPane;


/**
 * This process is used to import and prepare the tables (USR, ROADS and BUILDINGS) for a specified commune.
 * The user has to specify (mandatory):
 *  - A login and a password to the remote database
 *  - The INSEE identifier for the commune
 * @return a the tables USR, ROAD and BUILDING.
 *
 * @author Erwan Bocher
 */
@Process(title = "2-Import the USR, buildings and roads",
        resume = "Import the data for a specified zone. Note : The list of available communes must be already imported. If not please execute the script to import all commune areas...",
        keywords = "Vector,MAPuCE")
def processing() {
if(!login.isEmpty()&& !password.isEmpty()){

    logger.warn "Importing the USR from the remote database"
    sql.execute "DROP TABLE IF EXISTS USR_TEMP"
            def schemaFromRemoteDB = "lienss"	
            def tableFromRemoteDB = "(SELECT * FROM lienss.usr WHERE CODE_INSEE=''"+codeInsee[0]+"'')"
    def query = "CREATE LINKED TABLE USR_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
            sql.execute "drop table if exists USR_MAPUCE"
            sql.execute "CREATE TABLE USR_MAPUCE(PK serial NOT NULL PRIMARY KEY, THE_GEOM geometry, IDZONE integer NOT NULL, VEGETATION_SURFACE double, ROUTE_SURFACE double, HYDRO_SURFACE double, USR_AREA double, insee_individus double,insee_menages double ,insee_men_coll double ,insee_men_surf double ,insee_surface_collectif double,route_longueur double, trottoir_longueur double) AS SELECT PK, THE_GEOM, IDZONE, VEGETATION_SURFACE, ROUTE_SURFACE, HYDRO_SURFACE, ST_AREA(THE_GEOM) as USR_AREA , insee_individus,insee_menages,insee_men_coll,insee_men_surf ,insee_surface_collectif ,route_longueur , trottoir_longueur  FROM USR_TEMP"
            sql.execute "CREATE SPATIAL INDEX ON USR_MAPUCE(THE_GEOM);"
            sql.execute "DROP TABLE USR_TEMP"

            logger.warn "Importing the buildings from the remote database"
    sql.execute "DROP TABLE IF EXISTS BUILDINGS_TEMP"
            tableFromRemoteDB = "(SELECT a.*, b.CODE_INSEE  FROM lienss.BATI_TOPO a, lienss.ZONE_ETUDE b WHERE a.IDZONE = b.OGC_FID and b.CODE_INSEE=''"+codeInsee[0]+"'')"
    query = "CREATE LINKED TABLE BUILDINGS_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
    sql.execute "drop table if exists BUILDINGS_MAPUCE"
    sql.execute "CREATE TABLE BUILDINGS_MAPUCE (OGC_FID integer, THE_GEOM geometry, HAUTEUR_ORIGIN double, IDZONE integer, PK_USR integer, PK integer PRIMARY KEY, NB_NIV integer, HAUTEUR double, AREA double, PERIMETER double, L_CVX double, INSEE_INDIVIDUS double) AS SELECT OGC_FID, ST_NORMALIZE (THE_GEOM) as THE_GEOM, (HAUTEUR :: double) as HAUTEUR_ORIGIN, IDZONE, IDILOT as PK_USR, PK :: integer, NB_NIV, (HAUTEUR_CORRIGEE :: double) as HAUTEUR, ST_AREA(THE_GEOM) as AREA, ST_PERIMETER(THE_GEOM) as PERIMETER , ROUND(ST_PERIMETER(ST_CONVEXHULL(THE_GEOM)),3), INSEE_INDIVIDUS FROM BUILDINGS_TEMP WHERE ST_NUMGEOMETRIES(THE_GEOM)=1"

    sql.execute "UPDATE BUILDINGS_MAPUCE SET HAUTEUR=3 WHERE HAUTEUR is null OR HAUTEUR=0"
    sql.execute "UPDATE BUILDINGS_MAPUCE SET NB_NIV=1 WHERE NB_NIV=0"
    sql.execute "CREATE SPATIAL INDEX ON BUILDINGS_MAPUCE(THE_GEOM)"
    sql.execute "CREATE INDEX ON BUILDINGS_MAPUCE(PK_USR)"
    sql.execute "DROP TABLE  BUILDINGS_TEMP"

    logger.warn "Importing the roads from the remote database"
    sql.execute "DROP TABLE IF EXISTS ROADS_TEMP"
    schemaFromRemoteDB = "ign_bd_topo_2014"
    tableFromRemoteDB = "(SELECT * FROM ign_bd_topo_2014.ROUTE WHERE INSEECOM_D=''"+codeInsee[0]+"'' OR INSEECOM_G=''"+codeInsee[0]+"'')"
    query = "CREATE LINKED TABLE ROADS_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
    sql.execute "drop table if exists ROADS_MAPUCE"
    sql.execute "CREATE TABLE ROADS_MAPUCE (PK serial PRIMARY KEY, THE_GEOM geometry, LARGEUR double, INSEECOM_G varchar(5), INSEECOM_D varchar(5))AS SELECT PK , THE_GEOM, LARGEUR, INSEECOM_G, INSEECOM_D FROM ROADS_TEMP"
    sql.execute "CREATE SPATIAL INDEX ON ROADS_MAPUCE(THE_GEOM)"
    sql.execute "DROP TABLE IF EXISTS ROADS_TEMP"

    logger.warn "Keep the selected commune"
    sql.execute "drop table if exists COMMUNE_MAPUCE"
    query = "CREATE TABLE COMMUNE_MAPUCE AS SELECT * FROM COMMUNES_MAPUCE WHERE CODE_INSEE='"+codeInsee[0]+"'"
    sql.execute query
    literalOutput = "The data have been imported and prepared."
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

/** The list of Commune identifier */
@FieldValueInput(title="Commune identifier",
resume="Select the code insee of a commune to import the data.",
dataFieldTitle = "\$communes_mapuce\$code_insee\$",
multiSelection = false)
String[] codeInsee


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        resume="The output message")
String literalOutput
