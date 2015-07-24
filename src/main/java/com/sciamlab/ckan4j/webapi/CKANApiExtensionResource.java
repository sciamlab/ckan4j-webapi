package com.sciamlab.ckan4j.webapi;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sciamlab.ckan4j.CKANApiClient;
import com.sciamlab.ckan4j.CKANApiExtension;
import com.sciamlab.ckan4j.CKANApiClient.CKANApiClientBuilder;
import com.sciamlab.ckan4j.CKANApiExtension.App;
import com.sciamlab.ckan4j.CKANApiExtension.CKANApiExtensionBuilder;
import com.sciamlab.ckan4j.util.CKAN;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.exception.InternalServerErrorException;
import com.sciamlab.common.exception.NotFoundException;
import com.sciamlab.common.util.SciamlabCollectionUtils;


/**
 * Root parentResource (exposed at "printer" path)
 */
@Path("ext")
@Produces(MediaType.APPLICATION_JSON+"; charset=UTF-8")
@Consumes(MediaType.APPLICATION_JSON) 
@PermitAll
public class CKANApiExtensionResource {
	
	private static final Logger logger = Logger.getLogger(CKANApiExtensionResource.class);
	
	private CKANWebApiDAO dao = CKANWebApiDAO.getInstance();
	private CKANApiExtension ckanapiext;
	private CKANApiClient ckan;
	
	public CKANApiExtensionResource() throws MalformedURLException{
		this.ckanapiext = CKANApiExtensionBuilder.getInstance(dao).build();
		this.ckan = CKANApiClientBuilder.getInstance(CKANWebApiConfig.CKAN_ENDPOINT+"/api/3").apiKey(CKANWebApiConfig.CKAN_APIKEY).build();
	}
	
	@GET
    public Response apiInfo() {
		try{ 
			JSONObject info = new JSONObject();
			info.put("name", "ckan4j-webapi");
			info.put("version", "2.0");
			info.put("author", "SciamLab");
			info.put("contact", "api@sciamlab.com");
			JSONArray methods = new JSONArray().put("tags/top").put("datasets/count").put("organization/{name}").put("organizations/count").put("organizations/lastupdate").put("eurovoc/stats");
			info.put("methods", methods);
			return Response.ok(info.toString()).build();
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("apps")
	@GET
    public Response getApps(@QueryParam(value = "q") String q_string, @QueryParam(value = "t") List<String> types, @QueryParam(value = "lang") String lang) {
		try{
			List<App> apps = ckanapiext.getApps(q_string, types, lang);
			JSONArray apps_json = new JSONArray();
			for(App p : apps){
				JSONObject app  = new JSONObject();
				app.put("type", p.type);
				app.put("title", p.title);
				app.put("description", p.description);
				app.put("url", p.url);
				app.put("image_url", p.image_url);
				apps_json.put(app);
			}
			return Response.ok(apps_json.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("tags/top")
	@GET
    public Response getTopTags(@QueryParam(value = "limit") Integer limit) {
		try{
			if(limit == null) limit = 10;
			JSONObject toptags = new JSONObject();
			JSONArray array = new JSONArray();
			toptags.put("tags", array);
			Map<String, Integer> map = ckanapiext.getTagsCount(limit);
			for(String tag : map.keySet()){
				JSONObject t = new JSONObject();
				t.put("tag", tag);
				t.put("count", map.get(tag));
				array.put(t);
			}
			return Response.ok(toptags.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("publisher/{publisher}/reports")
	@GET
    public Response getCatalogReports(@PathParam(value = "publisher") String publisher) {
		try{
			JSONObject result = new JSONObject();
			JSONObject ds = this.ckan.packageShow(CKANWebApiConfig.PUBLISHER_REPORTS_DATASET);
			JSONObject org = this.ckan.organizationShow(publisher, false);
			for(Object t1 : SciamlabCollectionUtils.asList(org.getJSONArray(CKAN.EXTRAS))){
				JSONObject ex = (JSONObject)t1;
				String key = ex.getString(CKAN.KEY);
				if(!key.startsWith("catalog_"))
					continue;
				key = key.replace("catalog_", "");
				for(Object t2 : SciamlabCollectionUtils.asList(ds.getJSONArray(CKAN.RESOURCES))){
					JSONObject res = (JSONObject)t2;
					if(!res.getString(CKAN.NAME).contains(key))
						continue;
					result.put(key, new JSONObject().put(CKAN.ID, res.getString(CKAN.ID)).put(CKAN.NAME, res.getString(CKAN.NAME)));
				}
			}
			return Response.ok(result.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("datasets/count")
	@GET
    public Response getDatasetCount() {
		try{
			
			return Response.ok(new JSONObject().put("count", ckanapiext.getDatasetCount()).toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}

	@Path("datasets/stats")
	@GET
    public Response getDatasetStats() {
		try{
			JSONObject result = new JSONObject();
			Map<String,String> resultData = ckanapiext.getDatasetStats();
			for (String dsType : resultData.keySet()) {
				result.put(dsType, resultData.get(dsType)+"");
			}
			return Response.ok(result.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("eurovoc/stats")
	@GET
    public Response getEurovocStatData() {
		try{
			JSONObject result = new JSONObject();
			result.put("name", "EurovocClassificationBubble"); //root node
			JSONArray childrenThemesArray = new JSONArray(); // prepare the main eurovoc themes array
			
			Map<String,Map<String,Integer>> eurovocMap = ckanapiext.getEurovocMapData();
			for (String theme : eurovocMap.keySet() ) {
				JSONObject themeObj = new JSONObject();
				themeObj.put("name", theme);
				themeObj.put("style", theme.toLowerCase().replaceAll(",","").replaceAll(" ", "-"));				

				Map<String, Integer> microthemeData = eurovocMap.get(theme);
				JSONArray childrenMicrothemesArray = new JSONArray();
				for (String microtheme : microthemeData.keySet()) {
					JSONObject microthemeObj = new JSONObject();
					microthemeObj.put("name", microtheme);
					microthemeObj.put("size",microthemeData.get(microtheme));
					childrenMicrothemesArray.put(microthemeObj);
				}
				themeObj.put("children", childrenMicrothemesArray);
				childrenThemesArray.put(themeObj);
			}
			result.put("children", childrenThemesArray);

			return Response.ok(result.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	private static final Map<String, String> CATEGORIES = new HashMap<String, String>(){{
		put("AGRICULTURE-FISHERIES-FORESTRY-FOOD", "Agricoltura, pesca, natura, cibo");//"Agriculture, fisheries, forestry, food");
		put("EDUCATION-CULTURE-AND-SPORT", "Istruzione, cultura e sport");//"Education, culture and sport");
		put("ENVIRONMENT", "Ambiente");//"Environment");
		put("ENERGY", "Energia");//"Energy");
		put("TRANSPORT", "Trasporto");//"Transport");
		put("SCIENCE-AND-TECHNOLOGY", "Scienza e tecnologia");//"Science and technology");
		put("ECONOMY-AND-FINANCE", "Economia e finanza");//"Economy and finance");
		put("POPULATION-AND-SOCIAL-CONDITIONS", "Popolazione e welfare");//"Population and social conditions");
		put("HEALTH", "Salute");//"Health");
		put("GOVERNMENT-PUBLIC-SECTOR", "Governo, pubblica amministrazione ");//"Government, public sector");
		put("REGIONS-CITIES", "Regioni e città");//"Regions, cities");
		put("JUSTICE-LEGAL-SYSTEM-PUBLIC-SAFETY", "Giustizia e sicurezza");//"Justice, legal system, public safety");
		put("INTERNATIONAL-ISSUES", "Politica internazionale");//"International issues");
	}};
	
	@Path("category/facets")
	@GET
    public Response getCategoryfacets() {
		try{
			JSONArray result = new JSONArray();
			List<Properties> res = dao.execQuery( "select pe.value as name, count(*) as c from package p join package_extra pe on p.id = pe.package_id where p.state='active' and pe.key='dcat-category-id' group by pe.value order by c desc");
			for(Properties p : res) {
				JSONObject f = new JSONObject();
				f.put("count", p.get("c"));
				f.put("display_name", (CATEGORIES.containsKey(p.getProperty("name")))?CATEGORIES.get(p.getProperty("name")):p.getProperty("name"));
				f.put("name", p.getProperty("name"));
				result.put(f);
			}
			return Response.ok(result.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("organizations/count")
	@GET
    public Response getOrgCount() {
		try{
			Map<String, Integer> map = ckanapiext.getOrganizationCountMap();
			JSONArray result = new JSONArray();
			for (String catalog : map.keySet()) 
				result.put(new JSONObject().put("name", catalog).put("count", map.get(catalog)));
			return Response.ok(result.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@Path("organizations/lastupdate")
	@GET
    public Response getOrgLastUpdateMap() {
		try{
			Map<String, Date> map = ckanapiext.getOrganizationLastUpdateMap();
			JSONArray result = new JSONArray();
			for (String catalog : map.keySet())
				result.put(new JSONObject().put("name", catalog).put("date", map.get(catalog)));
			return Response.ok(result.toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}

	@Path("organization/{name}")
	@GET
    public Response getOrganizationDetails(@PathParam("name") String name) {
		try{
			String orgId = ckanapiext.getOrganizationId(name);
			if(orgId==null)
				throw new NotFoundException("Organization "+name+" not found");
			Date lastUpdate = ckanapiext.getOrganizationLastUpdate(name);
			Integer count = ckanapiext.getOrganizationCount(name);
			return Response.ok(new JSONObject().put("id", orgId).put("name", name).put("date", lastUpdate).put("count", count).toString()).build();
		} catch (NotFoundException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
}
