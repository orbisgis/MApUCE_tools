
import org.orbisgis.mapuce.WpsScriptsPackage;
import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*


import javax.swing.JOptionPane
import org.orbisgis.mapuce.randomForest.*
import java.sql.ResultSet
import java.sql.Statement
import java.io.File
import java.io.InputStreamReader
import java.net.URL
import java.sql.Connection
import org.apache.commons.io.FileUtils


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
    resume = "This process is used to classify the building and USR features using a RandomForest algorithm.",
    keywords = ["Vector","MAPuCE"])
def processing() {

    logger.warn "Download the MAPuCE model used to classify the buildings."

    //Do not download the file is already exist
    File file = new File(System.getProperty("user.home") + "/mapuce/mapuce-rf-1.0.RData");
    
    if(!file.exists()){
        FileUtils.copyURLToFile(new URL("https://github.com/orbisgis/MApUCE_tools/raw/master/model/mapuce-rf-1.0.RData"), file)   
    }
    
    sql.execute "drop table if exists TMP_TYPO_BUILDINGS_MAPUCE, TMP_TYPO_USR_MAPUCE,TYPO_BUILDINGS_MAPUCE, TYPO_USR_MAPUCE";
    sql.execute "create table TMP_TYPO_BUILDINGS_MAPUCE(pk integer, typo varchar)"
    sql.execute "create table TMP_TYPO_USR_MAPUCE(pk_usr integer, typo_maj varchar, typo_second varchar)"
    
    engine = rEngine.getScriptEngine();
    
    engine.put("con", rEngine.getConnectionRObject(sql.getDataSource().getConnection())); 
    engine.put("model_path", file.getAbsolutePath())
    
    r = WpsScriptsPackage.class.getResourceAsStream("scripts/randomforest_typo.R")
    
    
    engine.eval(new InputStreamReader(r));
    
    

    literalOutput = "The classification has been done. The tables USR_TYPO and BUILDING_TYPO have been created correctly" 
}


/** String output of the process. */
@LiteralDataOutput(
    title="Output message",
    resume="The output message")
String literalOutput

