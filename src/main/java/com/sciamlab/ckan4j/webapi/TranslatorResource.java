package com.sciamlab.ckan4j.webapi;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sciamlab.auth.annotation.ApiKeyAuthentication;
import com.sciamlab.ckan4j.CKANTranslator;
import com.sciamlab.ckan4j.CKANTranslator.CKANTranslatorBuilder;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.common.exception.BadRequestException;
import com.sciamlab.common.exception.DAOException;
import com.sciamlab.common.exception.InternalServerErrorException;

/**
 * 
 * @author SciamLab
 *
 */

@Path("translate")
@Produces(MediaType.APPLICATION_JSON+"; charset=UTF-8")
@Consumes(MediaType.APPLICATION_JSON+"; charset=UTF-8")
@PermitAll
@ApiKeyAuthentication
public class TranslatorResource {
	
	private static final Logger logger = Logger.getLogger(TranslatorResource.class);
	
	private CKANWebApiDAO dao = CKANWebApiDAO.getInstance();
	private CKANTranslator translator;
	
	public TranslatorResource(){
		translator = CKANTranslatorBuilder.getInstance(dao).build();
	}
	
	@GET
	@Path("term/list")
	public Response getTermsList(
    		@QueryParam("q") String q, 
    		@QueryParam("l") String lang_code, 
    		@QueryParam("tr") boolean translated, 
    		@QueryParam("ntr") boolean not_translated, 
    		@QueryParam("e") List<String> package_extra_keys,
    		@QueryParam("pn") Integer page_num,
    		@QueryParam("ps") Integer page_size){
		
		Map<String, Properties> terms;
		try {
			terms = translator.getTerms(q, lang_code, translated, not_translated, package_extra_keys, page_num, page_size);
		} catch (DAOException e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
		JSONArray array = new JSONArray();
		for(Properties p : terms.values()){
			JSONObject json = new JSONObject();
	    	json.put("term", p.get("term"));
			json.put("lang_code", p.get("lang_code"));
			json.put("term_translation", p.get("term_translation"));
			array.put(json);
		}
		logger.debug(array);
		return Response.ok(array.toString()).build();
	}
	
	@GET
	@Path("term/count")
    public Response getTermsCount(
    		@QueryParam("q") String q, 
    		@QueryParam("l") String lang_code, 
    		@QueryParam("tr") boolean translated, 
    		@QueryParam("ntr") boolean not_translated, 
    		@QueryParam("e") List<String> package_extra_keys){
		
		int count;
		try {
			count = translator.getTermsCount(q, lang_code, translated, not_translated, package_extra_keys);
		} catch (DAOException e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
		JSONObject json = new JSONObject();
    	json.put("count", count);
		return Response.ok(json.toString()).build();
	}
	
	@POST
	@Path("term/insert")
	@RolesAllowed({"admin","editor"})
    public Response doTranslation(String body) {
		JSONObject json = new JSONObject(body);
		
		if(json.opt("term")==null || "".equals(json.getString("term")))
			throw new BadRequestException("Param term must be provided");
		if(json.opt("lang_code")==null || "".equals(json.getString("lang_code")))
			throw new BadRequestException("Param lang_code must be provided");
		if(json.opt("term_translation")==null || "".equals(json.getString("term_translation")))
			throw new BadRequestException("Param term_translation must be provided");
		
		String term = json.getString("term");
		String lang_code = json.getString("lang_code");
		String term_translation = json.getString("term_translation");
		
		boolean success = false;
		try {
			translator.translate(term, lang_code, term_translation);
			success = true;
		} catch (DAOException e) {
			logger.error(e.getMessage(), e);
		}
		json.put("success", success);
		return Response.ok(json.toString()).build();
	}

}
