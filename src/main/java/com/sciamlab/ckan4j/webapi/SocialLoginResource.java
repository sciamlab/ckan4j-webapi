package com.sciamlab.ckan4j.webapi;

import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.CKAN_ENDPOINT;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.FACEBOOK_API_CLIENT_ID;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.FACEBOOK_API_ENDPOINT;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.FACEBOOK_API_SECRET;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GITHUB_API_CLIENT_ID;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GITHUB_API_ENDPOINT;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GITHUB_API_OAUTH_ENDPOINT;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GITHUB_API_SECRET;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GOOGLE_API_APP_NAME;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GOOGLE_API_CLIENT_ID;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GOOGLE_API_OAUTH_ENDPOINT;
import static com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig.GOOGLE_API_SECRET;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.UUID;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.plus.Plus;
import com.google.api.services.plus.model.Person;
import com.sciamlab.ckan4j.CKANLogin;
import com.sciamlab.ckan4j.CKANLogin.CKANLoginBuilder;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.exception.ForbiddenException;
import com.sciamlab.common.exception.InternalServerErrorException;
import com.sciamlab.common.exception.MethodNotAllowedException;
import com.sciamlab.common.exception.SciamlabWebApplicationException;
import com.sciamlab.common.util.HTTPClient;

@Path("auth")
@Produces(MediaType.TEXT_HTML)
@Consumes(MediaType.APPLICATION_JSON)
@PermitAll
public class SocialLoginResource {
	
	private static final Logger logger = Logger.getLogger(SocialLoginResource.class);
	
	private static final String GPLUS = "gplus";
	private static final String FACEBOOK = "facebook";
	private static final String GITHUB = "github";
	private static final String NOMAIL = "nomail@sciamlab.com";
	private CKANLogin ckan_login;
	
	
	private HTTPClient http = new HTTPClient();
	private CKANWebApiDAO dao = CKANWebApiDAO.getInstance();
	
	public SocialLoginResource(){
		this.ckan_login = CKANLoginBuilder.getInstance(CKANWebApiConfig.CKAN_ENDPOINT, CKANWebApiConfig.CKAN_LOGIN_SECRET).build();
	}
	
	@GET
    @Path("github")
    public Response doGETLoginGITHUB(@Context UriInfo uriInfo) {
		logger.debug(uriInfo.getRequestUri());
		final String redirect_uri = uriInfo.getAbsolutePath().toString();
		MultivaluedMap<String, String> query = uriInfo.getQueryParameters();
		for(String k : query.keySet()){
			logger.debug(k+": "+query.get(k));
		}
		try {
			if(!query.containsKey("code"))
				return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
			
			final String code = query.getFirst("code");
			
			//Identity confirmation
			String identity_confirmation = http.doGET(new URL(GITHUB_API_OAUTH_ENDPOINT),
					new MultivaluedHashMap<String, String>(){{
						add("client_id", GITHUB_API_CLIENT_ID);
						add("redirect_uri", redirect_uri);
						add("client_secret", GITHUB_API_SECRET);
						add("code", code);
					}}, 
					new MultivaluedHashMap<String, String>(){{
						add("Accept", "application/json");
					}}).readEntity(String.class);
			logger.debug(identity_confirmation);
			JSONObject json_identity_confirmation = new JSONObject(identity_confirmation);
			final String accessToken = json_identity_confirmation.getString("access_token");
//			logger.debug(accessToken);
			
			//Getting user details
			String person = http.doGET(new URL(GITHUB_API_ENDPOINT+"/user"), 
//					new MultivaluedHashMap<String, String>(){{
//						add("access_token", accessToken);
//					}},
					null,
					new MultivaluedHashMap<String, String>(){{
						add("Authorization", "token "+accessToken);
					}}).readEntity(String.class);
			logger.debug(person);
			JSONObject json_person = new JSONObject(person);
			String email = json_person.optString("email");
			String user = json_person.getString("login");
			String fullname = json_person.optString("name");
			String id = json_person.getInt("id")+"";
			return this.doGenericLogin(id, user, fullname, email, GITHUB, json_person);
			
		} catch (SciamlabWebApplicationException e) {
			logger.error(e.getErrorResponse(), e);
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
    }
	
	@GET
    @Path("facebook")
    public Response doGETLoginFACEBOOK(@Context UriInfo uriInfo) {
		logger.debug(uriInfo.getRequestUri());
		final String redirect_uri = uriInfo.getAbsolutePath().toString();
		MultivaluedMap<String, String> query = uriInfo.getQueryParameters();
		for(String k : query.keySet()){
			logger.debug(k+": "+query.get(k));
		}
		try {
			if(query.containsKey("error"))
				return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
			
			final String code = query.getFirst("code");
			
			//Identity confirmation
			String identity_confirmation = http.doGET(new URL(FACEBOOK_API_ENDPOINT+"/oauth/access_token"),
					new MultivaluedHashMap<String, String>(){{
						add("client_id", FACEBOOK_API_CLIENT_ID);
						add("redirect_uri", redirect_uri);
						add("client_secret", FACEBOOK_API_SECRET);
						add("code", code);
					}}, null).readEntity(String.class);
			logger.debug(identity_confirmation);
			if(!identity_confirmation.contains("access_token") && !identity_confirmation.contains("expires"))
				return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
			final String accessToken = identity_confirmation.substring("access_token=".length(), identity_confirmation.indexOf("&expires="));
//			logger.debug(accessToken);
			
			//Inspecting access token
			String inspection_confirmation = http.doGET(new URL(FACEBOOK_API_ENDPOINT+"/debug_token"), 
					new MultivaluedHashMap<String, String>(){{
						add("input_token", accessToken);
						add("access_token", FACEBOOK_API_CLIENT_ID+"|"+FACEBOOK_API_SECRET);
					}}, null).readEntity(String.class);
			logger.debug(inspection_confirmation);
			JSONObject json_inspection_confirmation = new JSONObject(inspection_confirmation);
			
			//Getting user details
			String person = http.doGET(new URL(FACEBOOK_API_ENDPOINT+"/me"), 
					new MultivaluedHashMap<String, String>(){{
						add("access_token", accessToken);
					}}, null).readEntity(String.class);
			logger.debug(person);
			JSONObject json_person = new JSONObject(person);
			String email = json_person.getString("email");
			if(email==null)
				throw new ForbiddenException("Email null from facebook response");
			if(email.contains("\u0040"))
				email = email.replaceAll("u0040", "@");
			String user = email;
			String fullname = json_person.getString("name");
			String id = json_person.getString("id");
			return this.doGenericLogin(id, user, fullname, email, FACEBOOK, json_person);
			
		} catch (SciamlabWebApplicationException e) {
			logger.error(e.getErrorResponse(), e);
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
		
    }
	
    @GET
    @Path("gplus")
    public Response doGETLoginGPLUS(@Context UriInfo uriInfo) {
    	logger.debug(uriInfo.getRequestUri());
		MultivaluedMap<String, String> query = uriInfo.getQueryParameters();
		for(String k : query.keySet()){
			logger.debug(k+": "+query.get(k));
		}
		try {
			if(query.containsKey("error"))
				return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
			
			String code = query.getFirst("code");
		
			String result = http.doPOST(new URL(GOOGLE_API_OAUTH_ENDPOINT), 
				  "code="+code
				+ "&client_id="+GOOGLE_API_CLIENT_ID
				+ "&client_secret="+GOOGLE_API_SECRET
				+ "&redirect_uri="+uriInfo.getAbsolutePath()
				+ "&grant_type=authorization_code", MediaType.APPLICATION_FORM_URLENCODED_TYPE, null, null).readEntity(String.class);
			logger.debug(result);
			JSONObject json_result = new JSONObject(result);
			String accessToken = json_result.getString("access_token");
			logger.debug("ACCESS TOKEN: "+accessToken);
			GoogleCredential credential = new GoogleCredential().setAccessToken(accessToken);
			Plus plus = new Plus.Builder(new NetHttpTransport(), new JacksonFactory(), credential)
				.setApplicationName(GOOGLE_API_APP_NAME).build();
			Person person;
			JSONObject json_person = new JSONObject();
			String email;
			String id;
			String user;
			String fullname;
			person = plus.people().get("me").execute();
			for(String k : person.keySet()){
				json_person.put(k, person.get(k));
			}
			logger.debug("PERSON: "+json_person);	
			email = (person.getEmails()!=null && !person.getEmails().isEmpty())
					?person.getEmails().get(0).getValue():null;
			if(email==null)
				throw new ForbiddenException("Email null from google response");
			user = email;
			id = person.getId();
			fullname = person.getDisplayName();
			return this.doGenericLogin(id, user, fullname, email, GPLUS, json_person);
		
		} catch (SciamlabWebApplicationException e) {
			logger.error(e.getErrorResponse(), e);
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
    }
    
	public Response doGenericLogin(String id, String user, String fullname, String email, final String social, final JSONObject json_person){
		final String id_social = id + "@" + social;
		//check user on ckan
		boolean exists = false;
		String check_user; 
		try {
			logger.debug(CKAN_ENDPOINT + "/api/3/action/user_show?id="+id_social);
			check_user = this.http.doGET(new URL(CKAN_ENDPOINT + "/api/3/action/user_show?id="+id_social), null, 
					new MultivaluedHashMap<String, String>(){{ 
						put("Authorization", new ArrayList<String>(){{ 
							add(CKANWebApiConfig.CKAN_APIKEY); }}); }}).readEntity(String.class);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
		logger.debug(check_user);
		JSONObject json;
		try {
			json = new JSONObject(check_user);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
		if(json.getBoolean("success")){ 
			if(id_social.equals(json.getJSONObject("result").getString("id"))){
				if("deleted".equals(json.getJSONObject("result").getString("state"))){
					throw new ForbiddenException("That login name is not available");
				}
				exists = true;
				user = json.getJSONObject("result").getString("name");
			}
			if(!json.getJSONObject("result").has("email")){
				throw new ForbiddenException("Access denied: User not authorized to create users via the API");
			}
		}
		JSONObject upsert_ckan_user_body = new JSONObject();
		upsert_ckan_user_body.put("id", id_social);
		if(email==null || "".equals(email))
			email = NOMAIL;
		upsert_ckan_user_body.put("email", email);
		if(fullname==null || "".equals(fullname))
			fullname = "";
		upsert_ckan_user_body.put("fullname", fullname);
		
		String ckan_user = null;
		String action;
		boolean skip = false;
		//if exists		--> update user on ckan
		//if not exists --> create user on ckan
		if(exists)	{
			action = "user_update";
			if(fullname.equals(json.getJSONObject("result").getString("fullname")) 
				&& email.equals(json.getJSONObject("result").getString("email")) )
				skip=true; //nothing to be updated....
		}else{
			action = "user_create";
			user = UUID.randomUUID().toString();
			upsert_ckan_user_body.put("name", user);
			upsert_ckan_user_body.put("password", user);
		}
		if(!skip){
			try {
				logger.debug(CKAN_ENDPOINT + "/api/3/action/"+action);
				ckan_user = this.http.doPOST(new URL(CKAN_ENDPOINT + "/api/3/action/"+action), 
						upsert_ckan_user_body.toString(), MediaType.APPLICATION_FORM_URLENCODED_TYPE, null, 
						new MultivaluedHashMap<String, String>(){{ 
							put("Authorization", new ArrayList<String>(){{ 
								add(CKANWebApiConfig.CKAN_APIKEY); }}); }}).readEntity(String.class);
				logger.debug(ckan_user);
				JSONObject create_user_json = new JSONObject(ckan_user);
				if(!create_user_json.getBoolean("success"))
					throw new ForbiddenException("Error during user creation on CKANApiClient: "+create_user_json.getString("error"));
				logger.info("User "+(("user_create".equals(action))?"created":"updated")+" on CKANApiClient: "+create_user_json.getJSONObject("result"));
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				throw new InternalServerErrorException(e);
			}
		}
		
		try {
			//update social details into user_social
			int r = dao.execUpdate("UPDATE user_social SET details = cast(? AS json) WHERE ckan_id = ? AND social = ?;", 
					new ArrayList<Object>(){{add(json_person.toString());add(id_social);add(social);}});
			if(r==0){
				logger.debug("No user found on user_extended with ckan id="+id_social);
				//insert social details into user_extended
				int r2 = dao.execUpdate("INSERT INTO user_social (ckan_id, social, details) VALUES (?, ?, cast(? AS json));", 
						new ArrayList<Object>(){{add(id_social);add(social);add(json_person.toString());}});
			}

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
			//if exception --> remove user on CKANApiClient
//			String delete_user = null; 
//			try {
//				JSONObject body = new JSONObject();
//				body.put("id", ckan_id);
//				delete_user = this.http.doPOST(new URL(CKAN_ENDPOINT + "/api/3/action/user_delete"), 
//						body.toString(), MediaType.APPLICATION_FORM_URLENCODED_TYPE, null, 
//						new MultivaluedHashMap<String, String>(){{ 
//							put("Authorization", new ArrayList<String>(){{ 
//								add(AuthLibConfig.CKAN_APIKEY); }}); }});
//			} catch (Exception e1) { 
//				//noop
//			}
//			logger.debug(delete_user);
//			throw new InternalServerErrorException("Error updating/inserting social user. The created ckan user "+ckan_id+" has been deleted (rollback)");
		}
		//return auto-submit login form
    	return ckan_login.login(user);
    }
	
    @GET
    @Path("logout")
    public Response doGETLogout() {
    	return ckan_login.logout();
    }
	
    @POST
    public Response doPOST() {
		throw new MethodNotAllowedException();
    }
    
    @GET
    public Response doGET() {
		throw new MethodNotAllowedException();
    }
    
    @PUT
    public Response doPUT() {
		throw new MethodNotAllowedException();
    }
	
    @DELETE
    public Response doDELETE() {
		throw new MethodNotAllowedException();
    }
    


    /*
     * Backup del draft dell'integrazione TWITTER
     */
    
//  public static String hmacSha1(String value, String key) {
//  try {
//      // Get an hmac_sha1 key from the raw key bytes
//      byte[] keyBytes = key.getBytes();           
//      SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA1");
//
//      // Get an hmac_sha1 Mac instance and initialize with the signing key
//      Mac mac = Mac.getInstance("HmacSHA1");
//      mac.init(signingKey);
//
//      // Compute the hmac on input data bytes
//      byte[] rawHmac = mac.doFinal(value.getBytes());
//
//      // Convert raw bytes to Hex
//      byte[] hexBytes = new Hex().encode(rawHmac);
//
//      //  Covert array of Hex bytes to a String
//      return new String(hexBytes, "UTF-8");
//  } catch (Exception e) {
//      throw new RuntimeException(e);
//  }
//}

//	@GET
//    @Path("twitter/request_token")
//    public Response doGETLoginTWITTER_REQUEST_TOKEN(@Context UriInfo uriInfo) {
//		try{
//			//%3D =
//			//%26 &
//			long timestamp = new Date().getTime()/1000;
//			String signature_base = "POST&https%3A%2F%2Fapi.twitter.com%2Foauth%2Frequest_token&"
//					+ "oauth_callback%3Dhttp%3A%2F%2Faltamira.duckdns.org:8080%2FCKANAPIExtension%2Fauth%2Ftwitter"
//					+ "%26oauth_consumer_key%3D"+TWITTER_API_CLIENT_ID
//					+ "%26oauth_nonce%"+TWITTER_NONCE
//					+ "%26oauth_signature_method%3DHMAC-SHA1"
//					+ "%26oauth_timestamp%3D"+timestamp
//					+ "%26oauth_version%3D1.0";
//			String signing_key = TWITTER_API_SECRET+"&";
//			String oauth_signature = hmacSha1(signature_base, signing_key); 
//			logger.debug("oauth_signature: "+oauth_signature);
////			return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
//			final String authorization = "OAuth oauth_callback=\"http://altamira.duckdns.org:8080/CKANAPIExtension/auth/twitter\""
//					+ ",oauth_consumer_key=\""+TWITTER_API_CLIENT_ID+"\""
//					+ ",oauth_nonce=\""+TWITTER_NONCE+"\""
//					+ ",oauth_signature_method=\"HMAC-SHA1\""
//					+ ",oauth_timestamp=\""+timestamp+"\""
//					+ ",oauth_version=\"1.0\""
//					+ ",oauth_signature=\""+oauth_signature+"\"";
//			
//			String request_token = http.doPOST(new URL(TWITTER_API_ENDPOINT+"/oauth/request_token"),
//					"", MediaType.APPLICATION_FORM_URLENCODED_TYPE,
//					null, 
//					new MultivaluedHashMap<String, String>(){{
//						add("Authorization", authorization);
//					}});
//			logger.debug(request_token);
////			if(!request_token.contains("oauth_token")
////					&& !request_token.contains("oauth_token_secret")
////					&& !request_token.contains("oauth_callback_confirmed"))
//				return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
////			
////			String oauth_token = request_token.split("&")[0].split("=")[0];
////			logger.debug("OAUTH_TOKEN: "+oauth_token);
////			return Response.temporaryRedirect(
////				new URI(TWITTER_API_ENDPOINT+"/oauth/authenticate?oauth_token="+oauth_token)).build();
//		} catch (Exception e) {
//			throw new InternalServerErrorException(SciamlabStringUtils.stackTraceToString(e));
//		}
//	}
	
//	@GET
//    @Path("twitter")
//    public Response doGETLoginTWITTER(@Context UriInfo uriInfo) {
//		logger.debug(uriInfo.getRequestUri());
//		final String redirect_uri = uriInfo.getAbsolutePath().toString();
//		MultivaluedMap<String, String> query = uriInfo.getQueryParameters();
//		for(String k : query.keySet()){
//			logger.debug(k+": "+query.get(k));
//		}
//		try {
//			if(!query.containsKey("oauth_token") || !query.containsKey("oauth_verifier"))
//				return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
//			
//			return Response.temporaryRedirect(new URI(CKAN_ENDPOINT+"/user/login")).build();
////			final String code = query.getFirst("code");
////			
////			//Identity confirmation
////			String identity_confirmation = http.doGET(new URL(GITHUB_API_OAUTH_ENDPOINT),
////					new MultivaluedHashMap<String, String>(){{
////						add("client_id", GITHUB_API_CLIENT_ID);
////						add("redirect_uri", redirect_uri);
////						add("client_secret", GITHUB_API_SECRET);
////						add("code", code);
////					}}, 
////					new MultivaluedHashMap<String, String>(){{
////						add("Accept", "application/json");
////					}});
////			logger.debug(identity_confirmation);
////			JSONObject json_identity_confirmation = (JSONObject) JSONSerializer.toJSON(identity_confirmation);
////			final String accessToken = json_identity_confirmation.getString("access_token");
//////			logger.debug(accessToken);
////			
////			//Getting user details
////			String person = http.doGET(new URL(GITHUB_API_ENDPOINT+"/user"), 
//////					new MultivaluedHashMap<String, String>(){{
//////						add("access_token", accessToken);
//////					}},
////					null,
////					new MultivaluedHashMap<String, String>(){{
////						add("Authorization", "token "+accessToken);
////					}});
////			logger.debug(person);
////			JSONObject json_person = (JSONObject)JSONSerializer.toJSON(person);
////			String email = json_person.getString("email");
////			String user = json_person.getString("login");
////			String fullname = json_person.getString("name");
////			String id = json_person.getString("id");
////			return this.doGenericLogin(id, user, fullname, email, GITHUB, json_person);
////			
//		} catch (Exception e) {
//			throw new InternalServerErrorException(SciamlabStringUtils.stackTraceToString(e));
//		}
//		
//    }
}
