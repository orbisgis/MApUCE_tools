package org.orbisgis.mapuce.scripts

import org.orbisgis.mapuce.WpsScriptsPackage;
import org.orbisgis.orbiswps.groovyapi.input.*
import org.orbisgis.orbiswps.groovyapi.output.*
import org.orbisgis.orbiswps.groovyapi.process.*


import javax.swing.JOptionPane
import org.orbisgis.mapuce.randomForest.*
import java.sql.ResultSet
import java.sql.Statement
import java.io.File
import java.io.InputStreamReader
import java.net.URL
import java.sql.Connection
import org.apache.commons.io.FileUtils
import org.orbisgis.rscriptengine.REngineFactory



/**
 * This process is used to classify the building and USR features using a RandomForest algorithm.
 *
 *
 * @return The table whith an ID and the class of each buildings
 *
 * @author Melvin Le Gall
 * @author Erwan Bocher
 */
@Process(title = "4-Classify the BUILDING and USR features",
        description = "This process is used to classify the building and USR features using a RandomForest algorithm.",
        keywords = ["Vector","MAPuCE"])
def processing() {
    
    def modelName = "mapuce-rf-2.2.RData";

    logger.warn i18n.tr("Download the MAPuCE model - {0} - used to classify the buildings.", modelName)
    
    

    //Do not download the file is already exist
    File file = new File(System.getProperty("user.home") + "/mapuce/"+ modelName );
    
    if(!file.exists()){
        FileUtils.copyURLToFile(new URL("https://github.com/orbisgis/MApUCE_tools/raw/master/model/$modelName"), file)   
    }
    
    logger.warn i18n.tr("Download model finished. Start classification.")
    
    sql.execute "drop table if exists TMP_TYPO_BUILDINGS_MAPUCE, TMP_TYPO_USR_MAPUCE,TYPO_BUILDINGS_MAPUCE, TYPO_USR_MAPUCE, typo_label";
    sql.execute "create table TMP_TYPO_BUILDINGS_MAPUCE(pk integer, typo varchar)"
    sql.execute "create table TMP_TYPO_USR_MAPUCE(pk_usr integer,ba float,bgh float,icif float, icio float, id float, local float, pcif float, pcio float, pd float, psc float , typo_maj varchar, typo_second varchar)"
    
    
    
    rEngine.put("con", REngineFactory.getConnectionRObject(sql.getDataSource().getConnection())); 
    rEngine.put("model_path", file.getAbsolutePath())
    
    r = WpsScriptsPackage.class.getResourceAsStream("scripts/randomforest_typo.R")

    logger.warn i18n.tr("Start classification.")
    
    rEngine.eval(new InputStreamReader(r));
    
    logger.warn i18n.tr("End classification.")
    
    // Create final tables with geometries
    sql.execute "CREATE INDEX ON TMP_TYPO_BUILDINGS_MAPUCE(PK); CREATE TABLE TYPO_BUILDINGS_MAPUCE AS SELECT a.the_geom, b.pk as pk_building, a.pk_usr, a.id_zone, b.typo from BUILDINGS_MAPUCE a, TMP_TYPO_BUILDINGS_MAPUCE b where a.pk=b.pk; "
    sql.execute "CREATE INDEX ON TMP_TYPO_USR_MAPUCE(PK_USR); CREATE TABLE TYPO_USR_MAPUCE AS SELECT a.the_geom, b.pk_usr, a.id_zone,b.ba,b.bgh,b.icif, b.icio, b.id, b.local, b.pcif, b.pcio, b.pd , b.psc , b.typo_maj, b.typo_second  from USR_MAPUCE a, TMP_TYPO_USR_MAPUCE b where a.pk=b.pk_usr;"
    sql.execute "DROP TABLE IF EXISTS TMP_TYPO_BUILDINGS_MAPUCE, TMP_TYPO_USR_MAPUCE, buildings_to_predict;"
    
    //Create a table to store typo label    
    sql.execute "create table typo_label (typo varchar(5), label varchar)"
    sql.execute "insert into typo_label VALUES ('bgh','Bâtiment de grande hauteur')"
    sql.execute "insert into typo_label VALUES ('pcio','Pavillon continu sur îlot ouvert')"
    sql.execute "insert into typo_label VALUES ('pd' ,'Pavillon discontinu')"
    sql.execute "insert into typo_label VALUES ('local' , 'Local annexe')"
    sql.execute "insert into typo_label VALUES ('pcif' , 'Pavillon continu sur îlot fermé')"
    sql.execute "insert into typo_label VALUES ('icif', 'Immeuble continu sur îlot fermé')"
    sql.execute "insert into typo_label VALUES ('psc', 'Pavillon semi-continu')"
    sql.execute "insert into typo_label VALUES ('icio','Immeuble continu sur îlot ouvert')"
    sql.execute "insert into typo_label VALUES ('ba', 'Bâtiment d''activité')"
    sql.execute "insert into typo_label VALUES ('id' , 'Immeuble discontinu')"

    literalOutput = i18n.tr("The classification has been done. The tables USR_TYPO and BUILDING_TYPO have been created correctly")
}


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        description="The output message")
String literalOutput

