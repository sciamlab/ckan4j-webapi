package com.sciamlab.ckan4j.webapi.dao;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.sciamlab.auth.dao.CKANLocalUserDAO;
import com.sciamlab.ckan4j.webapi.util.CKANWebApiConfig;
import com.sciamlab.common.exception.DAOException;

public class CKANWebApiDAO extends CKANLocalUserDAO {
	
	private static final Logger logger = Logger.getLogger(CKANWebApiDAO.class);

	private DataSource ds;
	
	private static volatile CKANWebApiDAO instance = null;
	
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
	
}