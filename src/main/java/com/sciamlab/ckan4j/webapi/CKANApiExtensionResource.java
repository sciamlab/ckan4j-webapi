package com.sciamlab.ckan4j.webapi;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
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
import com.sciamlab.ckan4j.CKANApiClient.CKANApiClientBuilder;
import com.sciamlab.ckan4j.CKANApiExtension;
import com.sciamlab.ckan4j.CKANApiExtension.App;
import com.sciamlab.ckan4j.util.CKAN;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.exception.DAOException;
import com.sciamlab.common.exception.web.InternalServerErrorException;
import com.sciamlab.common.exception.web.NotFoundException;
import com.sciamlab.common.util.Pair;
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
		this.ckanapiext = new CKANApiExtension.Builder(dao).build();
		this.ckan = CKANApiClientBuilder.init(CKANWebApiConfig.CKAN_ENDPOINT+"/api/3").apiKey(CKANWebApiConfig.CKAN_API_KEY).build();
	}
	
	@GET
    public Response apiInfo() {
		try{ 
			JSONObject info = new JSONObject();
			info.put("name", "ckan4j-webapi");
			info.put("version", "2.0");
			info.put("author", "SciamLab");
			info.put("contact", "api@sciamlab.com");
			JSONArray methods = new JSONArray()
					.put("tags/top")
					.put("datasets/count")
					.put("datasets/stats")
					.put("organization/{name}")
					.put("organizations/count")
					.put("organizations/lastupdate")
					.put("eurovoc/stats")
					.put("stats/dimension");
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
			
			return Response.ok(new JSONObject()
				.put("count", ckanapiext.getDatasetCount())
				.toString()).build();
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
			Map<String,Long> resultData = ckanapiext.getDatasetStats();
			for (String dsType : resultData.keySet()) {
				result.put(dsType, resultData.get(dsType));
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
				themeObj.put("style", theme.toLowerCase().replace(",","").replace(" ", "-"));				

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
	
	@Path("category/facets")
	@GET
    public Response getCategoryfacets() {
		try{
			JSONArray result = new JSONArray();
			List<Properties> res = dao.execQuery( 
					"select pe.value as name, count(*) as c"
					+ " from package p join package_extra pe on p.id = pe.package_id"
					+ " where p.state='active' and pe.key='dcat-category-id'"
					+ " group by pe.value"
					+ " order by c desc");
			for(Properties p : res) {
				try {
					JSONObject f = new JSONObject();
					f.put("count", p.get("c"));
					f.put("display_name", CKANWebApiConfig.CATEGORIES.get(p.getProperty("name")).labels.get(Locale.ITALIAN.getISO3Language().toLowerCase()));
					f.put("name", p.getProperty("name"));
					result.put(f);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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

	@Path("stats/dimension")
	@GET
	public Response getDimensionAnalysis(@QueryParam("dim1") String dim1, @QueryParam("dim2") String dim2) {
		try{
			Map<Pair<String, String>, Integer> map = ckanapiext.getDimensionBasedStatistics(dim1, dim2);
			JSONArray json = new JSONArray();
			for(Pair<String, String> key : map.keySet())
				json.put(new JSONArray().put(key.getFirst()).put(key.getSecond()).put(map.get(key)));
			return Response.ok(json.toString()).build();
		} catch (DAOException e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e.getMessage());
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
