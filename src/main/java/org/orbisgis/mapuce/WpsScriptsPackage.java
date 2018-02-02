package org.orbisgis.mapuce;

import org.orbisgis.orbiswps.serviceapi.WpsScriptBundle;
import org.orbisgis.orbiswps.serviceapi.process.ProcessMetadata;
import org.orbisgis.rscriptengine.REngineFactory;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnap.commons.i18n.I18n;
import org.xnap.commons.i18n.I18nFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

/**
 * Main class of the plugin which declares the scripts to add, their locations in the process tree and the icons
 * associated.
 * In the WpsService, the script are organized in a tree, which has the WpsService as root.
 *
 * Scripts can be add to the tree under a specific node path with custom icon with the following method :
 *      wpsServer.addLocalScript('processFile', 'icon', 'boolean', 'nodePath');
 * with the following parameter :
 *      processFile : The File object corresponding to the script. Be careful, the plugin resource files can't be
 *              accessed from the outside of the plugin. So you have to copy it (in a temporary file as example) before
 *              adding it to the WpsService.
 *      icon : Array of Icon object to use for the WpsClient tree containing the processes. The first icon will be used
 *              for the first node of the path, the second icon for the second node ... If the node already exists,
 *              its icon won't be changed. If there is less icon than node, the last icon will be used for the others.
 *              If no icon are specified, the default one from the WpsClient will be used.
 *      boolean : it SHOULD be true. Else the used will be able to remove the process from the WpsClient without
 *              deactivating the plugin.
 *      nodePath : Path to the node where the process should be add. If nodes of the path doesn't exists, they will be
 *              created.
 * This add method return a ProcessIdentifier object which give all the information needed to identify a process. It
 * should be kept to be able to remove it later.
 *
 *
 * There is two method already implemented in this class to add the processes :
 *      Default method : the 'defaultLoadScript('nodePath')' methods which take as argument the nodePath and adds all
 *              the scripts from the 'resources' folder, keeping the file tree structure. All the script have the same
 *              icons.
 *      Custom method : the 'customLoadScript()' method load the scripts one by one under different file path with
 *              different icons.
 *
 *
 * When the plugin is launched , the 'activate()' method is call. This method load the scripts in the
 * WpsService and refresh the WpsClient.
 * When the plugin is stopped or uninstalled, the 'deactivate()' method is called. This method removes the loaded script
 * from the WpsService and refreshes the WpsClient.
 *
 */
@Component
public class WpsScriptsPackage implements WpsScriptBundle {

    protected static final Logger LOGGER = LoggerFactory.getLogger(WpsScriptsPackage.class);
    /** Resource path to the folder containing the scripts. */
    private static final String SCRIPTS_RESOURCE_FOLDER_PATH = "scripts";
    /** Resource path to the folder containing the icons. */
    private static final String ICONS_RESOURCE_FOLDER_PATH = "icons";
    /** Name of the icon file to use. */
    private static final String ICON_NAME = "mapuce.png";
    /** {@link I18n} object */
    private static I18n I18N;

    @Activate
    public void activate(){
        I18N = I18nFactory.getI18n(WpsScriptsPackage.class);
    }

    @Override
    public Map<String, Object> getGroovyProperties() {
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("rEngine", REngineFactory.createRScriptEngine());
        return propertiesMap;
    }

    @Override
    public List<URL> getScriptsList() {
        URL url = this.getClass().getResource(SCRIPTS_RESOURCE_FOLDER_PATH);
        List<URL> childUrl = new ArrayList<>();

        //Case of an osgi bundle
        Bundle bundle = FrameworkUtil.getBundle(this.getClass());
        if(bundle != null) {
            Enumeration<URL> enumUrl = bundle.findEntries(url.getFile(), "*", false);
            while(enumUrl.hasMoreElements()) {
                URL fileUrl = enumUrl.nextElement();
                if(fileUrl.getFile().endsWith(".groovy")) {
                    childUrl.add(fileUrl);
                }
            }
            return childUrl;
        }

        //Other case
        try {
            File f = new File(url.toURI());
            if(f.exists()){
                for(File child : f.listFiles()){
                    childUrl.add(child.toURI().toURL());
                }
                return childUrl;
            }
        } catch (URISyntaxException |MalformedURLException ignored) {}

        //Unknown case, return empty list
        LOGGER.error(I18N.tr("Unable to explore the URL {0}", url));
        return new ArrayList<>();
    }

    @Override
    public Map<ProcessMetadata.INTERNAL_METADATA, Object> getScriptMetadata(URL url) {
        Map<ProcessMetadata.INTERNAL_METADATA, Object> metadataMap = new HashMap<>();
        metadataMap.put(ProcessMetadata.INTERNAL_METADATA.IS_REMOVABLE, false);
        metadataMap.put(ProcessMetadata.INTERNAL_METADATA.NODE_PATH, "MAPuCE");
        URL[] icons = new URL[]{this.getClass().getResource(ICONS_RESOURCE_FOLDER_PATH+"/"+ICON_NAME)};
        metadataMap.put(ProcessMetadata.INTERNAL_METADATA.ICON_ARRAY, icons);
        return metadataMap;
    }

    @Override
    public I18n getI18n() {
        return I18N;
    }
}
