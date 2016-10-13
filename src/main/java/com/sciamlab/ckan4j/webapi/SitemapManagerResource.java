package com.sciamlab.ckan4j.webapi;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import com.sciamlab.ckan4j.CKANApiClient.CKANApiClientBuilder;
import com.sciamlab.ckan4j.CKANSitemapGenerator;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.exception.SciamlabException;
import com.sciamlab.common.exception.web.BadRequestException;
import com.sciamlab.common.exception.web.InternalServerErrorException;
import com.sciamlab.common.exception.web.SciamlabWebApplicationException;
import com.sciamlab.common.util.SciamlabStringUtils;

@Path("sitemap")
@Produces(MediaType.APPLICATION_XML)
public class SitemapManagerResource {

	private static final Logger logger = Logger.getLogger(SitemapManagerResource.class);

	CKANSitemapGenerator gen;
	
	public SitemapManagerResource() throws FileNotFoundException, MalformedURLException, SciamlabException {
		gen = new CKANSitemapGenerator.Builder(
				CKANWebApiConfig.CKAN_ENDPOINT,
				CKANApiClientBuilder.init(CKANWebApiConfig.CKAN_API_ENDPOINT).apiKey(CKANWebApiConfig.CKAN_API_KEY).build())
				.sitemap_template_file("sitemap-template.xml")
				.languages(Arrays.asList(CKANWebApiConfig.CKAN_LANGUAGES.split(","))).language("").build();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response apiInfo() {
		return Response.ok(SciamlabStringUtils.getSciamlabMethodsInfo(SitemapManagerResource.class).toString()).build();
	}

	@GET
	@Path("{name}.xml")
	public Response getSitemapMain(@PathParam("name") String name, @Context UriInfo uri) {
		return getSitemap(name, 0, uri);
	}
	@GET
	@Path("{name}.xml/{i}")
	public Response getSitemap(@PathParam("name") String name, @PathParam("i") Integer i, @Context UriInfo uri) {
		try{
			List<StringBuffer> xml = gen.generate(name, uri.getBaseUriBuilder().path(uri.getPathSegments().get(0).toString()).build().toString());
			if(i>=xml.size())
				throw new BadRequestException("sitemap index out of bound ["+xml.size()+"]");
			return Response.ok(xml.get(i).toString()).build();

		}catch(SciamlabWebApplicationException e){
			logger.error(e.getMessage());
			throw e;
		}catch(Exception e){
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e.getMessage());
		}
	}
	
}
