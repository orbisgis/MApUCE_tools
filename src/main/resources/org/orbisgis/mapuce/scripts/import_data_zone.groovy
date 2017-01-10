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
        description = "Import the data (USR, buildings and roads) needed to compute morphological indicators for a specified commune.<br> The imported data are stored into a remote database. Please contact info@orbigis.org to obtain an account. <br> Note : The list of available communes must be already imported. If not please execute the script to import all commune areas...",
        keywords = ["Vector","MAPuCE"])
def processing() {
if(!login.isEmpty()&& !password.isEmpty()){
    login = login.trim();
    password = password.trim();

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
            sql.execute "CREATE TABLE USR_MAPUCE(PK integer, THE_GEOM geometry, ID_ZONE integer NOT NULL, VEGETATION_SURFACE double precision, ROUTE_SURFACE double precision, HYDRO_SURFACE double precision, USR_AREA double precision, insee_individus double precision,insee_menages double precision,insee_men_coll double precision,insee_men_surf double precision,insee_surface_collectif double precision,route_longueur double precision, trottoir_longueur double precision) AS SELECT PK, THE_GEOM, IDZONE as ID_ZONE, VEGETATION_SURFACE, ROUTE_SURFACE, HYDRO_SURFACE, ST_AREA(THE_GEOM) as USR_AREA , insee_individus,insee_menages,insee_men_coll,insee_men_surf ,insee_surface_collectif ,route_longueur , trottoir_longueur  FROM USR_TEMP"
            sql.execute "CREATE SPATIAL INDEX ON USR_MAPUCE(THE_GEOM);"
            sql.execute "CREATE  INDEX ON USR_MAPUCE(PK);"
            sql.execute "DROP TABLE USR_TEMP"

            logger.warn "Importing the buildings from the remote database"
    sql.execute "DROP TABLE IF EXISTS BUILDINGS_TEMP"
            tableFromRemoteDB = "(SELECT a.IDZONE, a.THE_GEOM, a.HAUTEUR, a.IDILOT as PK_USR, a.PK, a.NB_NIV, a.HAUTEUR_CORRIGEE, a.INSEE_INDIVIDUS, a.THEME,a.PAI_BDTOPO,a.PAI_NATURE, b.CODE_INSEE  FROM lienss.BATI_TOPO a, lienss.ZONE_ETUDE b WHERE a.IDZONE = b.OGC_FID and b.CODE_INSEE=''"+codeInsee[0]+"'' and ST_NUMGEOMETRIES(a.the_geom)=1 and ST_ISEMPTY(a.the_geom)=false)"
    query = "CREATE LINKED TABLE BUILDINGS_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
    sql.execute "drop table if exists BUILDINGS_MAPUCE"
    sql.execute "CREATE TABLE BUILDINGS_MAPUCE (THE_GEOM geometry, HAUTEUR_ORIGIN double precision, ID_ZONE integer, PK_USR integer, PK integer PRIMARY KEY, NB_NIV integer, HAUTEUR double precision, AREA double precision, PERIMETER double precision, L_CVX double precision, INSEE_INDIVIDUS double precision, THEME varchar, PAI_BDTOPO varchar, PAI_NATURE varchar ) AS SELECT  ST_NORMALIZE (THE_GEOM) as THE_GEOM, (HAUTEUR :: double precision) as HAUTEUR_ORIGIN, IDZONE as ID_ZONE, PK_USR, PK :: integer, NB_NIV, (HAUTEUR_CORRIGEE :: double precision) as HAUTEUR, ST_AREA(THE_GEOM) as AREA, ST_PERIMETER(THE_GEOM) as PERIMETER , ROUND(ST_PERIMETER(ST_CONVEXHULL(THE_GEOM)),3), INSEE_INDIVIDUS, THEME , PAI_BDTOPO, PAI_NATURE FROM BUILDINGS_TEMP"

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
    sql.execute "CREATE TABLE ROADS_MAPUCE (PK integer, THE_GEOM geometry, LARGEUR double precision, INSEECOM_G varchar(5), INSEECOM_D varchar(5))AS SELECT PK , THE_GEOM, LARGEUR, INSEECOM_G, INSEECOM_D FROM ROADS_TEMP"
    sql.execute "CREATE SPATIAL INDEX ON ROADS_MAPUCE(THE_GEOM)"
    sql.execute "DROP TABLE IF EXISTS ROADS_TEMP"
    
    logger.warn "Importing the geometry of the spatial unit"
    
    sql.execute "DROP TABLE IF EXISTS COMMUNE_MAPUCE_TEMP, COMMUNE_MAPUCE"
    schemaFromRemoteDB = "lienss"
    tableFromRemoteDB = "(SELECT the_geom, CODE_INSEE, unite_urbaine FROM lienss.zone_etude WHERE CODE_INSEE=''"+codeInsee[0]+"'')"
    query = "CREATE  LINKED TABLE COMMUNE_MAPUCE_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";    
    sql.execute query
    sql.execute "CREATE TABLE COMMUNE_MAPUCE AS SELECT * FROM COMMUNE_MAPUCE_TEMP"
    	
    sql.execute "DROP TABLE IF EXISTS COMMUNE_MAPUCE_TEMP"
        
    
    literalOutput = "The data have been successfully imported."
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

/** The list of Commune identifier */
@JDBCTableFieldValueInput(title="Commune identifier",
        description="Select the code insee of a commune to import the data.",
        jdbcTableFieldReference = "\$communes_mapuce\$code_insee\$",
        multiSelection = false)
String[] codeInsee


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        description="The output message")
String literalOutput
