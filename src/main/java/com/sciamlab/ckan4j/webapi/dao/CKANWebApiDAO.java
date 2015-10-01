package com.sciamlab.ckan4j.webapi.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.postgresql.util.PGobject;

import com.sciamlab.auth.dao.SciamlabAuthDAO;
import com.sciamlab.auth.model.Role;
import com.sciamlab.auth.model.User;
import com.sciamlab.auth.model.UserLocal;
import com.sciamlab.auth.model.UserSocial;
import com.sciamlab.auth.util.AuthLibConfig;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.dao.SciamlabDAO;
import com.sciamlab.common.exception.DAOException;

public class CKANWebApiDAO extends SciamlabDAO implements SciamlabAuthDAO {
	
	private static final Logger logger = Logger.getLogger(CKANWebApiDAO.class);

	private DataSource ds;
	
	private static volatile CKANWebApiDAO instance = null;
	
	public static Map<String,User> USERS_BY_NAME = new HashMap<String, User>();
		 
	private CKANWebApiDAO(String location) {	
		InitialContext cxt;
		try{
			cxt = new InitialContext();
		
			if ( cxt == null ) 
				throw new DAOException("Uh oh -- no context!"); 
			
			ds = (DataSource) cxt.lookup(location);
			
			if ( ds == null ) 
				throw new DAOException("Data source not found!");
			
		}catch(NamingException e){
			throw new DAOException(e);
		}
	}
 
	public static CKANWebApiDAO getInstance() {
		if (instance == null) {
			synchronized (CKANWebApiDAO.class) {
				if (instance == null)
					instance = new CKANWebApiDAO(CKANWebApiConfig.DS_LOCATION);
			}
		}
		return instance;
	}

	@Override
	protected Connection getConnection() throws SQLException {
		return ds.getConnection();
	}
	
	public User getUserByName(String name) {
		User u = USERS_BY_NAME.get(name);
		if(u==null){
			u = (User)this.getUser("name", name);
			// updating cache
			USERS_BY_NAME.put(u.getName(), u);
		}
		return u;
	}
	
	public User getUserByApiKey(String apikey) {
		return this.getUser("apikey", apikey);
	}
	
	public User getUser(String col_key, final String col_value) {
		List<Properties> map = this.execQuery("SELECT * FROM "+AuthLibConfig.USERS_TABLE_NAME+" u left join "+AuthLibConfig.USERS_SOCIAL_TABLE_NAME+" us on u.id=us.ckan_id WHERE "+col_key+" = ?", 
				new ArrayList<Object>(){{ add(col_value); }},
				new ArrayList<String>(){{ add("id"); add("name"); add("password"); add("fullname"); add("email"); add("apikey"); add("social"); add("details");  }}); 
		if(map.size()==0) return null;
		if(map.size()>1) 
			throw new DAOException("Multiple users retrieved using "+col_key+": "+col_value);
		Properties p = map.get(0);
		User u = null;
		if(p.containsKey("social")){
			u = new UserSocial();
			((UserSocial) u).setSocialType(UserSocial.TYPES.get(p.getProperty("social")));
			((UserSocial) u).setSocialDetails(new JSONObject(((PGobject) p.get("details")).getValue()));
		}else{
			u = new UserLocal();
			((UserLocal) u).setFirstName(p.getProperty("fullname"));
			((UserLocal) u).setEmail(p.getProperty("email"));
			((UserLocal) u).setPassword(p.getProperty("password"));
		}
		u.setApiKey(p.getProperty("apikey"));
		u.setId(p.getProperty("name"));
		u.getRoles().clear();
		u.getRoles().addAll(this.getRolesByUserId(p.getProperty("id")));
		u.getProfiles().clear();
		u.getProfiles().putAll(this.getProfilesByUserId(p.getProperty("id")));
		logger.debug("User: "+u);
        return u;
	}
	
	public List<User> getUserList() {
		List<Properties> map = this.execQuery("SELECT * FROM "+AuthLibConfig.USERS_TABLE_NAME+" u left join "+AuthLibConfig.USERS_SOCIAL_TABLE_NAME+" us on u.id=us.ckan_id", 
				null,
				new ArrayList<String>(){{ add("id"); add("name"); add("fullname"); add("email"); add("apikey"); add("social"); add("details"); }}); 
		List<User> users = new ArrayList<User>();
		for(Properties p : map){
			User u = null;
			if(p.containsKey("social")){
				u = new UserSocial();
				((UserSocial) u).setSocialType(UserSocial.TYPES.get(p.getProperty("social")));
				((UserSocial) u).setSocialDetails(new JSONObject(((PGobject) p.get("details")).getValue()));
			}else{
				u = new UserLocal();
				((UserLocal) u).setFirstName(p.getProperty("fullname"));
				((UserLocal) u).setEmail(p.getProperty("email"));
			}
			u.setApiKey(p.getProperty("apikey"));
			u.setId(p.getProperty("name"));
			u.getRoles().clear();
			u.getRoles().addAll(this.getRolesByUserId(p.getProperty("id")));
			u.getProfiles().clear();
			u.getProfiles().putAll(this.getProfilesByUserId(p.getProperty("id")));
			logger.debug("User: "+u);
			users.add(u);
		}
        return users;
	}
	
	public List<Role> getRolesByUserId(final String id) {
		Map<String, Properties> map = this.execQuery("SELECT DISTINCT role FROM "+AuthLibConfig.ROLES_TABLE_NAME+" WHERE user_id = ?",
				new ArrayList<Object>(){{ add(id); }}, "role", new ArrayList<String>());
		List<Role> roles = new ArrayList<Role>();
		if(map.size()==0) {
			roles.add(Role.anonymous);
			return roles;
		}
		
		for(String r : map.keySet()){
			Role role = Role.valueOf(r);
			if(role!=null){
				roles.add(role);
				logger.debug("Role added: "+r);
			}else{
				logger.warn(r+"is not identified as a valid role!");
			}
		}
		if(roles.isEmpty())
			roles.add(Role.anonymous);
		return roles;
	}
	
	/**
	 * get the user profiles on all the apis
	 * 
	 * @param user_id
	 * @return a map where the keys are the api names and the values are the related profiles
	 */
	public Map<String, String> getProfilesByUserId(final String user_id) {
		Map<String, Properties> result = this.execQuery("SELECT DISTINCT api,profile FROM "+AuthLibConfig.PROFILES_TABLE_NAME+" WHERE user_id = ?", 
				new ArrayList<Object>(){{ add(user_id); }}, "api", new ArrayList<String>(){{ add("api"); add("profile"); }});
		Map<String, String> profiles = new HashMap<String, String>();
		for(String api : AuthLibConfig.API_LIST){
			String profile = (result.containsKey(api)) ? result.get(api).getProperty("profile") : AuthLibConfig.API_BASIC_PROFILE;
			profiles.put(api, profile);
		}
		return profiles;
	}
	
	/**
	 * set the user profile on the given api
	 * 
	 * @param user_id
	 * @param api
	 * @param profile
	 * @return number of updated records
	 */
	public int setUserAPIProfile(final String user_id, final String api, final String profile) {
		int result = this.execUpdate("UPDATE "+AuthLibConfig.PROFILES_TABLE_NAME+" SET profile = ?, modified = ? WHERE user_id = ? AND api = ?",
				new ArrayList<Object>() {{ add(profile); add(new java.sql.Date(new Date().getTime())); add(user_id); add(api);}});
		if(result==0){
			//profile not found for given api, need to insert
			result = this.execUpdate("INSERT INTO "+AuthLibConfig.PROFILES_TABLE_NAME+" VALUES (?, ?, ?, ? ,?)",
					new ArrayList<Object>() {{ add(user_id); add(api); add(profile); add(new java.sql.Date(new Date().getTime())); add(new java.sql.Date(new Date().getTime())); }});
		}
		return result;
	}
	
	/**
	 * delete the user profile on the given api
	 * 
	 * @param user_id
	 * @param api
	 * @param profile
	 * @return number of updated records
	 */
	public int deleteUserAPIProfile(final String user_id, final String api) {
		int result = this.execUpdate("DELETE FROM "+AuthLibConfig.PROFILES_TABLE_NAME+" WHERE user_id = ? AND api = ?",
				new ArrayList<Object>() {{ add(user_id); add(api); }});
		return result;
	}

	@Override
	public User validate(String jwt) {
		throw new RuntimeException("method not implemented!!");
	}

}