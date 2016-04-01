package com.sciamlab.ckan4j.webapi;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sciamlab.common.exception.web.InternalServerErrorException;


/**
 * Root parentResource (exposed at "printer" path)
 */
@Path("")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON) 
@PermitAll
public class CKANWebApiResource {
	
	private static final Logger logger = Logger.getLogger(CKANWebApiResource.class);
	
	@GET
    public Response apiInfo() {
		try{ 
			JSONObject info = new JSONObject();
			info.put("name", "ckan4j-webapi");
			info.put("version", "2.0");
			info.put("author", "SciamLab");
			info.put("contact", "api@sciamlab.com");
			JSONArray methods = new JSONArray().put("ext").put("data").put("rate").put("auth").put("translate").put("user");
			return Response.ok(info.put("methods", methods).toString()).build();
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
}
