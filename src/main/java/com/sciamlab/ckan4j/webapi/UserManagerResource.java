package com.sciamlab.ckan4j.webapi;

import java.util.List;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sciamlab.auth.annotation.ApiKeyAuthentication;
import com.sciamlab.auth.model.User;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.common.exception.InternalServerErrorException;
import com.sciamlab.common.exception.SciamlabWebApplicationException;
import com.sciamlab.common.util.SciamlabCollectionUtils;

@Path("user")
@Produces(MediaType.APPLICATION_JSON)
//@Consumes(MediaType.APPLICATION_JSON)
@PermitAll
public class UserManagerResource {
	
	private static final Logger logger = Logger.getLogger(UserManagerResource.class);
	
	private CKANWebApiDAO dao = CKANWebApiDAO.getInstance();
	
	public UserManagerResource(){ }
	
	@Path("validate")
	@GET
    public Response validate(@QueryParam("key") String key, @HeaderParam("Authorization") String authorization) {
		try {
			if(authorization!=null)
				key = authorization;
			
			JSONObject result = new JSONObject();
			result.put("success", false);
			User user = dao.getUserByApiKey(key);
	        logger.debug("User: "+user);
	        
	        if(user==null)
	        	return Response.ok(result.toString()).build();

	        result.put("success", true);
	        result.put("user", user.toJSON());
			return Response.ok(result.toString()).build();
		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("list")
	@RolesAllowed("admin")
	@GET
	@ApiKeyAuthentication
    public Response getUserList() {
		try {
			JSONObject result = new JSONObject();
			result.put("success", false);
			List<User> users = dao.getUserList();
	        
	        result.put("success", true);
	        JSONArray array = new JSONArray();
	        for(User u : users){
	        	array.put(u.getName());
	        }
	        result.put("users", array);
			return Response.ok(result.toString()).build();
		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("{name}")
	@RolesAllowed("admin")
	@GET
	@ApiKeyAuthentication
    public Response getUser(@PathParam("name") String name) {
		try {
			JSONObject result = new JSONObject();
			result.put("success", false);
			User user = dao.getUser("name",name);
	        logger.debug("User: "+user);
	        
	        if(user==null)
	        	return Response.ok(result.toString()).build();

	        result.put("success", true);
	        result.put("user", user.toJSON());
			return Response.ok(result.toString()).build();
		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("{name}/profiles")
	@RolesAllowed("admin")
	@GET
	@ApiKeyAuthentication
    public Response getProfiles(@PathParam("name") String name) {
		try {
			JSONObject result = new JSONObject();
			result.put("success", false);
			User user = dao.getUserByName(name);
	        logger.debug("User: "+user);
	        
	        if(user==null)
	        	return Response.ok(result.toString()).build();

	        result.put("success", true);
			result.put("profiles", ((JSONObject)user.toJSON()).getJSONArray("profiles"));
			return Response.ok(result.toString()).build();
		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("{id}/profiles")
	@RolesAllowed("admin")
	@POST
	@ApiKeyAuthentication
    public Response setProfiles(@PathParam("id") String id, String body) {
		try {
			JSONObject result = new JSONObject();
			result.put("success", false);
			JSONArray profiles = new JSONArray(body);
			User user = null;
			for(Object tmp : SciamlabCollectionUtils.asList(profiles)){
				JSONObject profile = (JSONObject) tmp;
				user = dao.setUserProductProfile(id, profile.getString("api"), profile.getString("profile"));
			}

	        result.put("success", true);
	        result.put("profiles", ((JSONObject)user.toJSON()).getJSONArray("profiles"));
			return Response.ok(result.toString()).build();
		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("{id}/profiles")
	@RolesAllowed("admin")
	@DELETE
	@ApiKeyAuthentication
    public Response deleteProfiles(@PathParam("id") String id, String body) {
		try {
			JSONObject result = new JSONObject();
			result.put("success", false);
			JSONArray profiles = new JSONArray(body);
			User user = null;
			for(Object tmp : SciamlabCollectionUtils.asList(profiles)){
				JSONObject profile = (JSONObject) tmp;
				user = dao.deleteUserProductProfile(id, profile.getString("api"));
			}
	        result.put("success", true);
	        result.put("profiles", ((JSONObject)user.toJSON()).getJSONArray("profiles"));
			return Response.ok(result.toString()).build();
		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	

}
