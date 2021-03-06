package com.sciamlab.ckan4j.webapi;

import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;

import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.AnnotatedMethod;

import com.sciamlab.auth.annotation.ApiKeyAuthentication;
import com.sciamlab.auth.filter.TokenBasedSecurityFilter;
import com.sciamlab.auth.filter.TokenBasedSecurityFilter.TokenBasedSecurityFilterBuilder;
import com.sciamlab.auth.util.JerseyHelper;
import com.sciamlab.ckan4j.webapi.dao.CKANWebApiDAO;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;

public class CKANWebApiResourceConfig extends ResourceConfig {

	private static final Logger logger = Logger.getLogger(CKANWebApiResourceConfig.class);

	public CKANWebApiResourceConfig() {
		logger.info("Initializing "+CKANWebApiResourceConfig.class.getSimpleName()+"...");
		CKANWebApiConfig.init();
		final TokenBasedSecurityFilter tokenBasedSecurityFilter = TokenBasedSecurityFilterBuilder.init(CKANWebApiDAO.getInstance()).build();
		/*
		 * filter used to inject custom implementation of SecurityContext
		 */
		register(new DynamicFeature() {
			@Override
			public void configure(ResourceInfo resourceInfo, FeatureContext context) {
				if (!resourceInfo.getResourceClass().getSimpleName().endsWith("Resource")) {
					return;
				}
				 
				AnnotatedMethod am = new AnnotatedMethod(resourceInfo.getResourceMethod());
				logger.info("["+resourceInfo.getResourceClass().getSimpleName()+"."+resourceInfo.getResourceMethod().getName()+"] ");
				if (am.isAnnotationPresent(ApiKeyAuthentication.class)
						|| resourceInfo.getResourceClass().isAnnotationPresent(ApiKeyAuthentication.class)) {
					context.register(tokenBasedSecurityFilter);
		        }
			} 
		});
		
		//register custom RolesAllowedDynamicFeature
		JerseyHelper.registerCustomRolesAllowedDynamicFeature(this);

		packages("com.sciamlab.ckan4j.webapi");
		packages("com.sciamlab.auth.filter");
		logger.info("[DONE]");
	}
}