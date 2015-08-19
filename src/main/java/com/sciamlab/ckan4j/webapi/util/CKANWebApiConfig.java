package com.sciamlab.ckan4j.webapi.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONObject;

import com.sciamlab.common.exception.InternalServerErrorException;
import com.sciamlab.common.util.SciamlabStreamUtils;
import com.sciamlab.common.util.SciamlabStringUtils;

public class CKANWebApiConfig {
	
	private static final Logger logger = Logger.getLogger(CKANWebApiConfig.class);
	private static final String PROPS_FILE = "ckan4j.properties";
	private static final String DEFAULT_LOG_FILE = "log4j.properties";
	
	public static boolean LOG_ENABLED;
	
	public static String GOOGLE_API_APP_NAME;
	public static String GOOGLE_API_SECRET;
	public static String GOOGLE_API_CLIENT_ID;
	public static String GOOGLE_API_OAUTH_ENDPOINT;
	public static String FACEBOOK_API_SECRET;
	public static String FACEBOOK_API_CLIENT_ID;
	public static String FACEBOOK_API_ENDPOINT;
	public static String GITHUB_API_SECRET;
	public static String GITHUB_API_CLIENT_ID;
	public static String GITHUB_API_ENDPOINT;
	public static String GITHUB_API_OAUTH_ENDPOINT;
	public static String TWITTER_API_SECRET;
	public static String TWITTER_API_CLIENT_ID;
	public static String TWITTER_API_ENDPOINT;
	
	public static String CKAN_ENDPOINT;
	public static String DS_LOCATION;
	public static String SECURITY_DS_LOCATION;
	public static String CKAN_APIKEY;
	public static String RATING_TABLE;
	public static String PUBLISHER_REPORTS_DATASET;
	
	public static JSONObject CATEGORIES;
	public static String CATEGORIES_FILE;
	
	public static String CKAN_LOGIN_SECRET;
	
	static{
		try {
			PropertyConfigurator.configure(SciamlabStreamUtils.getInputStream(System.getProperty("logprops_filepath", DEFAULT_LOG_FILE)));
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage(), e);
		}
		logger.info("log4j config file successfully loaded");
		//loading properties
		try {
			loadProps();
			logger.info("Properties loading completed");
		} catch (Exception e) {
			logger.error("Error loading properties", e);
			throw new InternalServerErrorException(e);
		}		
		//loading categories
		logger.info("Loading Categories...");
		InputStream is = null;
		try {
			is = SciamlabStreamUtils.getInputStream(CATEGORIES_FILE);
			String categories_file_content = SciamlabStreamUtils.convertStreamToString(is);
			CATEGORIES = new JSONObject(categories_file_content);
			logger.info("Found "+CATEGORIES.getJSONArray("categories").length()+" categories");
        }catch(Exception e){
        	logger.error("Error loading categories", e);
			throw new InternalServerErrorException(e);
    	}finally {
            if (is != null)	try { is.close(); } catch (IOException e) { logger.error(e.getMessage(), e); }
        }
		
		logger.info("DONE");
		
	} 
	
	/**
	 * this is just to force the loading of static properties
	 */
	public static void init(){}
	
	public static void loadProps() throws IOException {
    	InputStream is = null;
		try {
			is = SciamlabStreamUtils.getInputStream(PROPS_FILE);
			Properties prop = new Properties();
			prop.load(is);
			
			for(Object k : prop.keySet()){
				logger.info(k+": "+prop.getProperty((String)k));
			}
			
			CATEGORIES_FILE = prop.getProperty("categories.json");
			CKAN_ENDPOINT = prop.getProperty("ckan.endpoint");
			DS_LOCATION = SECURITY_DS_LOCATION = prop.getProperty("ds.location");
			CKAN_APIKEY = prop.getProperty("ckan.apikey");
			RATING_TABLE = prop.getProperty("rating.table");
			
			GOOGLE_API_APP_NAME = prop.getProperty("google.api.app_name");
			GOOGLE_API_SECRET = prop.getProperty("google.api.secret");
			GOOGLE_API_CLIENT_ID = prop.getProperty("google.api.client_id");
			GOOGLE_API_OAUTH_ENDPOINT = prop.getProperty("google.api.oauth_endpoint");
			FACEBOOK_API_SECRET = prop.getProperty("facebook.api.secret");
			FACEBOOK_API_CLIENT_ID = prop.getProperty("facebook.api.client_id");
			FACEBOOK_API_ENDPOINT = prop.getProperty("facebook.api.endpoint");
			GITHUB_API_SECRET = prop.getProperty("github.api.secret");
			GITHUB_API_CLIENT_ID = prop.getProperty("github.api.client_id");
			GITHUB_API_OAUTH_ENDPOINT = prop.getProperty("github.api.oauth_endpoint");
			GITHUB_API_ENDPOINT = prop.getProperty("github.api.endpoint");
			TWITTER_API_SECRET = prop.getProperty("twitter.api.secret");
			TWITTER_API_CLIENT_ID = prop.getProperty("twitter.api.client_id");
			TWITTER_API_ENDPOINT = prop.getProperty("twitter.api.endpoint");
			
			CKAN_LOGIN_SECRET = prop.getProperty("ckan.login.secret");
			
			PUBLISHER_REPORTS_DATASET = prop.getProperty("publisher.reports.dataset");
			
//			//auth related
			com.sciamlab.auth.util.AuthLibConfig.USERS_TABLE_NAME = prop.getProperty("db.table.users");
			com.sciamlab.auth.util.AuthLibConfig.USERS_SOCIAL_TABLE_NAME = prop.getProperty("db.table.users_social");
			com.sciamlab.auth.util.AuthLibConfig.ROLES_TABLE_NAME = prop.getProperty("db.table.roles");
			com.sciamlab.auth.util.AuthLibConfig.PROFILES_TABLE_NAME = prop.getProperty("db.table.profiles");

		} finally {
			if (is != null) try { is.close(); } catch (IOException e) { logger.error(e.getMessage(),e); }
		}
		
	}
}
