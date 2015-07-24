package com.sciamlab.ckan4j.webapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.base.CharMatcher;
import com.sciamlab.common.exception.BadRequestException;
import com.sciamlab.common.exception.InternalServerErrorException;
import com.sciamlab.common.util.HTTPClient;
import com.sciamlab.common.util.SciamlabStreamUtils;
import com.sciamlab.common.util.SciamlabStringUtils;

@Path("proxy")
@Consumes(MediaType.APPLICATION_JSON)
@PermitAll
public class ProxyResource {
	
	private static final Logger logger = Logger.getLogger(ProxyResource.class);
	
//	private CKANWebApiDAO dao = CKANWebApiDAO.getInstance();
	private HTTPClient http = new HTTPClient();
	
	public ProxyResource(){ }
	
	//http://jsonpdataproxy.appspot.com/
	//?callback=jQuery171021386758820153773_1427806763686
	//&url=http%3A%2F%2Fwww.minambiente.it%2Fcanoni_locazione_affitto_attivi.csv%2F2014
	//&max-results=1000
	//&type=csv
	//&_=1427806763901 

	private static enum FileType{
		TSV("TSV","text/csv", "getCSVRecordsFromURL"),
		CSV("CSV","text/csv", "getCSVRecordsFromURL"),
		XLS("XLS","application/vnd.ms-excel", "getXLSRecordsFromURL"),
		XLSX("XLSX","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "getXLSXRecordsFromURL");
		
		private String media_type;
		private String type;
		private String method;
		
		FileType(String type, String media_type, String method) {
	        this.media_type = media_type;
	        this.type = type;
	        this.method = method;
		}
		
		public static Map<String, FileType> byType = new HashMap<String, ProxyResource.FileType>();
		
		public static FileType getByType(String type){ return byType.get(type); }
		
		static{
	    	for(FileType t : FileType.values()){
	    		byType.put(t.type, t);
	    	}
	    }
	}
	
	@Produces(MediaType.APPLICATION_JSON+"; charset=UTF-8")
	@Path("data")
	@GET
    public Response data(@QueryParam("callback") final String callback, @QueryParam("url") final String url, @QueryParam("type") final String type) {
		if(type==null)
			throw new BadRequestException("missing type parameter. use one in "+FileType.byType.keySet());
		if(!FileType.byType.containsKey(type.toUpperCase()))
			throw new BadRequestException("wrong file type. use one in "+FileType.byType.keySet());
		if(url==null)
			throw new BadRequestException("missing url parameter");
		
		try {
			String method = FileType.getByType(type.toUpperCase()).method;
			Map<Integer, List<Object>> records = 
					(Map<Integer, List<Object>>) ProxyResource.class.getMethod(method, String.class).invoke(this, url); 
			
			JSONObject json = new JSONObject(); 
            json.put("url", url);
            boolean first = true;
            JSONArray fields = new JSONArray();
			JSONArray data = new JSONArray();
			JSONObject metadata = new JSONObject();
			if(records != null){
				for(Integer i : records.keySet()){
					List<Object> r = records.get(i);
	        		if(first){
	        			first = false;
	        			JSONArray metadata_fields = new JSONArray();
	        			for(Object k : r){
	        				fields.put(k.toString());
	        				JSONObject f_json = new JSONObject();
	        				f_json.put("id", k.toString());
	        				metadata_fields.put(f_json);
	        			}
	        			metadata.put("fields", metadata_fields);
	        		}else{
	        			JSONArray row = new JSONArray();
	        			for(Object k : r){
	        				row.put(k);
	        			}
	        			data.put(row);
	        		}
	            }
			}
            json.put("fields", fields);
			json.put("data", data);
			json.put("metadata", metadata);
			return Response.ok(callback+"("+json.toString()+")").build();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new InternalServerErrorException(e);
		}
	}
	
//	/**
//	 * the method uses the convention proposed at http://www.w3.org/TR/2015/WD-csv2json-20150108/
//	 * @param url
//	 * @param type
//	 * @param header
//	 * @return
//	 */
//	@Path("proxy")
//	@GET
//    public Response proxy(@QueryParam("url") final String url, @QueryParam("type") final String type) {
//		if(type==null)
//			throw new BadRequestException("missing type parameter. use one in "+FileType.byType.keySet());
//		if(!FileType.byType.containsKey(type.toUpperCase()))
//			throw new BadRequestException("wrong file type. use one in "+FileType.byType.keySet());
//		if(url==null)
//			throw new BadRequestException("missing url parameter");
//		
//		try {
//			String method = FileType.getByType(type).method;
//			Map<Integer, List<String>> records = 
//					(Map<Integer, List<String>>) this.getClass().getMethod(method, String.class).invoke(this, url);
//
//			JSONObject distribution = new JSONObject().put("downloadURL", url);
//			JSONArray rows = new JSONArray();
//			boolean first = true;
//			List<String> header = null;
//			if(records != null)
//				for(Integer i : records.keySet()){
//					List<String> r = records.get(i);
//					if(first){
//						header = r;
//						first = false;
//					}else{
//						JSONObject row = new JSONObject();
//						Map<String, Integer> duplicate_headers = new HashMap<String, Integer>(); 
//						for(int ii=0 ; ii<header.size() ; ii++){
//							String key = header.get(ii);
//							if(duplicate_headers.containsKey(key)){
//								Integer count = duplicate_headers.get(key);
//								if(count==1)
//									row.put(key+"_1",row.remove(key));
//								duplicate_headers.put(key, count+1);
//								key = key+"_"+duplicate_headers.get(key);
//							}else{
//								duplicate_headers.put(key, 1);
//							}
//							row.put(key, r.get(ii));	
//						}
//						rows.put(row);
//					}
//				}
//			JSONObject json = new JSONObject().put("distribution", distribution).put("row", rows);
//			return Response.ok(json.toString()).build();
//		} catch (Exception e) {
//			logger.error(e.getMessage(), e);
//			throw new InternalServerErrorException(e);
//		}
//	}
	
	public Map<Integer, List<Object>> getXLSRecordsFromURL(String url) throws Exception{
		InputStream is = null;
        try {
        	is = SciamlabStreamUtils.getRemoteInputStream(url);
	        HSSFWorkbook wb = new HSSFWorkbook(is);
	        HSSFSheet sheet = wb.getSheetAt(0);
	        Iterator<Row> rowIterator = sheet.iterator();
	        return getGenericExcelRecordsUsingIterator(rowIterator);
        }finally {
            if (is != null)	try { is.close(); } catch (IOException e) { logger.error(SciamlabStringUtils.stackTraceToString(e)); }
        }
	}
	
	public Map<Integer, List<Object>> getXLSXRecordsFromURL(String url) throws Exception{
		InputStream is = null;
        try {
        	is = SciamlabStreamUtils.getRemoteInputStream(url);
			XSSFWorkbook wb = new XSSFWorkbook(is);
			XSSFSheet sheet = wb.getSheetAt(0);
			Iterator<Row> rowIterator = sheet.iterator();
			return getGenericExcelRecordsUsingIterator(rowIterator);
        }finally {
            if (is != null)	try { is.close(); } catch (IOException e) { logger.error(SciamlabStringUtils.stackTraceToString(e)); }
        }
	}
	
	private Map<Integer, List<Object>> getGenericExcelRecordsUsingIterator(Iterator<Row> rowIterator) {
		Map<Integer, List<Object>> records = new LinkedHashMap<Integer, List<Object>>();
		int r_num = 0;
		Cell cell;
        Row row;
        while (rowIterator.hasNext()) {
			row = rowIterator.next();
			List<Object> record = new ArrayList<Object>();
			Iterator<Cell> cellIterator = row.cellIterator();
			while (cellIterator.hasNext()) {
				cell = cellIterator.next();
				switch (cell.getCellType()) {
				case Cell.CELL_TYPE_BOOLEAN:
					record.add(cell.getBooleanCellValue());
					break;
				case Cell.CELL_TYPE_NUMERIC:
					record.add(cell.getNumericCellValue());
					break;
				case Cell.CELL_TYPE_STRING:
					record.add(cell.getStringCellValue());
					break;
				case Cell.CELL_TYPE_BLANK:
					record.add(" ");
					break;
				default:
					record.add(cell);
				}
			}
			records.put(r_num, record);
			r_num++;
   		}
		return records;
	}

	public Map<Integer, List<Object>> getCSVRecordsFromURL(String url) throws Exception{
		InputStream is = null;
        BufferedReader reader = null;
        try {
        	is = SciamlabStreamUtils.getRemoteInputStream(url);
        	InputStreamReader isr = new InputStreamReader(is, "UTF8");
        	logger.info("File encoding: "+isr.getEncoding());
            reader = new BufferedReader(isr);
            String line;
            Map<Integer, List<Object>> records = new LinkedHashMap<Integer, List<Object>>();
			String separator = null;
			int r_num = 0;
			while ((line = reader.readLine()) != null) {
    			if(separator==null){
					separator = getSeparator(line);
					logger.debug("using '"+separator+"' as separator");
    			}
    			List<Object> record = new ArrayList<Object>();
    			for(String c : line.split(separator)){
    				if(c.startsWith("\"") && c.endsWith("\""))
    					c = c.substring(1, c.length()-1);
    				try {
    					//checking if integer
    					String d = c;
    					if(d.split(",").length>2){
    						// 10,000,000
    						record.add(Integer.parseInt(d.replace(",", "")));
    					}else if(d.split(".").length>2){
    						// 10.000.000
    						record.add(Integer.parseInt(d.replace(".", "")));
    					}else if(!d.contains(",") && d.split(".").length==2){
    						// 10.000
    						record.add(Integer.parseInt(d.replace(".", "")));
    					}else{
    						record.add(Integer.parseInt(c));
    					}
					} catch (NumberFormatException e) {
						try {
							//checking if double
							String d = c;
							if(d.split(",").length>2){
								// 10,000,000.00
								record.add(Double.parseDouble(d.replace(",", "")));
							}else if(d.split(".").length>2){
								// 10.000.000,00
								record.add(Double.parseDouble(d.replace(".", "").replace(",", ".")));
							}else if(d.contains(".") && d.contains(",") && d.indexOf(".")<d.indexOf(",")){
								// 10.000,00
								record.add(Double.parseDouble(d.replace(".", "").replace(",", ".")));
							}else if(d.contains(".") && d.contains(",") && d.indexOf(".")>d.indexOf(",")){
								// 10,000.00
								record.add(Double.parseDouble(d.replace(",", "")));
							}else if(!d.contains(".") && d.split(",").length==2){
								// 10,00
								record.add(Double.parseDouble(d.replace(",", ".")));
							}else{
	    						record.add(Double.parseDouble(c));
	    					}
						} catch (NumberFormatException e1) {
							//otherwise string
							record.add(c);
						}
					}
    			}
    			records.put(r_num, record);
    			r_num++;
            }
			return records;
            
        }finally {
            if(is != null)	try { is.close(); } catch (IOException e) { logger.error(SciamlabStringUtils.stackTraceToString(e)); }
            if(reader!=null) try { reader.close(); } catch (IOException e) { logger.error(SciamlabStringUtils.stackTraceToString(e)); }
        }
	}
	
	private static String getSeparator(String line){
		int comma=CharMatcher.is(',').countIn(line);
		int semicolon=CharMatcher.is(';').countIn(line);
		int tab=CharMatcher.is('\t').countIn(line);
		return (comma>=semicolon)?(comma>=tab)?",":"\t":(semicolon>=tab)?";":"\t";
	}
}
