package com.sciamlab.ckan4j.webapi;

import java.net.MalformedURLException;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.sciamlab.ckan4j.CKANRating;
import com.sciamlab.ckan4j.CKANRating.CKANRatingBuilder;
import com.sciamlab.ckan4j.exception.CKANException;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.exception.InternalServerErrorException;

@Path("rate")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@PermitAll
public class RatingStarsResource {
	
	private static final Logger logger = Logger.getLogger(RatingStarsResource.class);
	
	private CKANWebApiDAO dao = CKANWebApiDAO.getInstance();
	private CKANRating ckan_rating;
	
	public RatingStarsResource() throws MalformedURLException { 
		ckan_rating = CKANRatingBuilder.getInstance(dao, CKANWebApiConfig.CKAN_ENDPOINT+"/api/3", CKANWebApiConfig.CKAN_API_KEY, CKANWebApiConfig.RATING_TABLE).build();
	}
	
	@GET
    @Path("{ds}")
	public Response getRate(@PathParam("ds") final String ds) {
		try {
			return Response.ok(ckan_rating.getRate(ds).toString()).build();
		} catch (CKANException e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
	@POST
    @Path("{ds}")
	public Response doRate(@PathParam("ds") final String ds, @QueryParam("user") final String user, @QueryParam("rating") final Integer rating) {
		try {
			return Response.ok(ckan_rating.postRate(ds, user, rating).toString()).build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
}
