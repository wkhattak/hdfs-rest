package com.khattak.bigdata.filesystems.hdfs;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.javatuples.Pair;

/*
 * Gets the 5 min consumption & forecast data from HDFS and then writes csv files to C:\inetpub\wwwroot\test\charts\smart_meter
 */
public class HDFSRest {	
	
	public static void main(String[] args) throws Exception {
		//Date theTime = new Date(1471422293105L);
		//time.setTime(theTime);
		
		if (args.length != 3){
			System.out.println("Provide the IP:Port of WebHDFS e.g. 192.168.70.134:50070 & HDFS directories e.g. /hadoop/data/smartmeter/statistics/city_usage/five_minute/2016/10/4/five_minute_statistics.csv /hadoop/data/smartmeter/forecasts/city_usage/output/five_minute_forecasts.csv");
		}
		else {
	        GetData consumption = new GetData(args[0], args[1], "consumption"); 
        	new Thread(consumption).start();
        	
        	GetData forecast = new GetData(args[0], args[2], "forecast"); 
        	new Thread(forecast).start();
		}			
	}

	/*
	 * Gets consumption data from HDFS, creates 1 csv for all cities & 1 per city
	 */
	private static void getConsumptionData(String hdfsLocation, String hdfsFilePath) {
		String hdfsEndpoint = "http://" + hdfsLocation + "/webhdfs/v1" + hdfsFilePath +"?op=OPEN";
		while(true){
			String fileData = getFileData(hdfsEndpoint);
			//System.out.println(fileData);		

			if (fileData != null && !fileData.isEmpty()) {
				writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\consumption_5_min.csv", fileData);
				System.out.println("File consumption_5_min.csv written at: " + (new Date()).toString());
				
				Map<String, List<Pair<String, String>>> areaData = ConvertToAreaData(fileData);
				
				for (Map.Entry<String, List<Pair<String, String>>> entry : areaData.entrySet()){
					String areaFileData = GenerateAreaFileData(entry.getKey(), entry.getValue());
					if (areaFileData != null && !areaFileData.isEmpty()) {
						writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\" + entry.getKey() + "_consumption_5_min.csv", areaFileData);
						System.out.println("File " + entry.getKey() + "_consumption_5_min.csv written at: " + (new Date()).toString());
					}
				}
			}

			System.out.println("*****");
			System.out.println("*****");
			
			try {
				Thread.sleep(15 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}
	
	/*
	 * Gets forecast data from HDFS, creates 1 csv for all cities & 1 per city
	 */
	private static void getForecastData(String hdfsLocation, String hdfsFilePath) {
		String hdfsEndpoint = "http://" + hdfsLocation + "/webhdfs/v1" + hdfsFilePath +"?op=OPEN";
		while(true){
			String fileData = getFileData(hdfsEndpoint);
			//System.out.println(fileData);		

			if (fileData != null && !fileData.isEmpty()) {
				writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\forecast_5_min.csv", fileData);
				System.out.println("File forecast_5_min.csv written at: " + (new Date()).toString());
				
				Map<String, List<Pair<String, String>>> areaData = ConvertToAreaData(fileData);
				for (Map.Entry<String, List<Pair<String, String>>> entry : areaData.entrySet()){
					
					String areaFileData = GenerateAreaFileData(entry.getKey(), entry.getValue());
					
					if (areaFileData != null && !areaFileData.isEmpty()) {
						writeFile("C:\\inetpub\\wwwroot\\test\\charts\\smart_meter\\" + entry.getKey() + "_forecast_5_min.csv", areaFileData);
						System.out.println("File " + entry.getKey() + "_forecast_5_min.csv written at: " + (new Date()).toString());
					}

				}
			}

			System.out.println("*****");
			System.out.println("*****");
			
			try {
				Thread.sleep(15 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
	}
	
	/*
	 * Gets data from HDFS file
	 */
	private static String getFileData(String hdfsEndpoint){
		String results = null;
		
		HttpClient client = new HttpClient();
        int statusCode = 0;
        GetMethod getMethod = new GetMethod(hdfsEndpoint);
        //getMethod.setRequestHeader(new Header("Accept", "text/xml"));
        //getMethod.setRequestHeader(new Header("Accept", "application/JSON"));
        try {
			statusCode = client.executeMethod(getMethod);
			if (statusCode != HttpStatus.SC_OK) {
	        	System.err.println("Get method failed: " + getMethod.getStatusLine() + "!!!");
	        	if (statusCode == HttpStatus.SC_NOT_FOUND) {
	        		System.err.println("File: " + hdfsEndpoint + " doesn't exist!!!");
	        	}
	        }
			else{
				
		        /*InputStream rstream = null;
		        rstream = getMethod.getResponseBodyAsStream();
		        BufferedReader br = new BufferedReader(new InputStreamReader(rstream));
		        StringBuilder resultBuilder = new StringBuilder();
		        String result = null;
		        while ((result = br.readLine()) != null) {
		        	resultBuilder.append(result);
		        }
		        br.close();
		        
		        results = resultBuilder.toString();*/
				results = getMethod.getResponseBodyAsString();
			}
		} catch (IOException e) {
			System.out.println("IO Exception!!!");
			e.printStackTrace();
		}
		
		return results;
	}
	
	private static void writeFile(String fileNameIncludingPath, String data){
		File file = null;
		FileWriter writer = null;
		try {
			file = new File(fileNameIncludingPath);
			if (file.exists()) file.delete();
			file.createNewFile();
			
		    writer = new FileWriter(file, false); 
		    writer.write(data); 
		    writer.flush();
		}
		catch (IOException exp){
			System.out.println("Error while writing hdfs results!!!");
		}
		finally{
			try {
				if (writer != null) writer.close();
			}
			catch (IOException exp){
				System.out.println("Error while closing hdfs result's file!!!");
			}
		}
	}

	private static class GetData implements Runnable {
		String hdfsLocation = null;
		 String hdfsFilePath = null;
		String dataType = null;
		
		public GetData(String hdfsLocation, String hdfsFilePath, String dataType){
			this.hdfsLocation = hdfsLocation;
			this.hdfsFilePath = hdfsFilePath;
			this.dataType = dataType;
		}
		
		@Override
		public void run() {
			switch (this.dataType) {
				case "consumption": getConsumptionData(this.hdfsLocation, this.hdfsFilePath);
				break;
				case "forecast": getForecastData(this.hdfsLocation, this.hdfsFilePath);
				break;
				default:;
				break;
			}
			
		}
	}

	/*
	 * Converts csv data to a map with keys for each city & values are Pairs(timestamp,value)
	 */
	private static Map<String, List<Pair<String, String>>> ConvertToAreaData(String fileData){
		
		Map<String, List<Pair<String, String>>>  areaStatisticsMapTemp = new HashMap<String, List<Pair<String, String>>>();
			
		String[] arrFileData = fileData.split("\n");// can't use System.getProperty("line.separator") as file was written in Linux OS that has a different line separator than Windows 
		String[] header = arrFileData[0].split(",");// first row is header row
			
		for (int j = 1; j < header.length; j++){// first col is Timestamp col
			areaStatisticsMapTemp.put(header[j].toLowerCase() + "|" + j, new ArrayList<Pair<String, String>>());
		}
		
		for (int i = 1; i < arrFileData.length; i++){// first row is header row
			for (int j = 1; j < header.length; j++){// first col is Timestamp col
				areaStatisticsMapTemp.get(header[j].toLowerCase() + "|" + j).add(new Pair<String, String>(arrFileData[i].split(",")[0], arrFileData[i].split(",")[j]));
			}
		}
		
		Map<String, List<Pair<String, String>>>  areaStatisticsMap = new HashMap<String, List<Pair<String, String>>>();
		// clear map key before returning
		for (Map.Entry<String, List<Pair<String, String>>> entry : areaStatisticsMapTemp.entrySet()){
			areaStatisticsMap.put(entry.getKey().split("\\|")[0], entry.getValue());
		}
		
		return areaStatisticsMap;
    }

    /*
     * Creates data in csv format for the passed in city
     */
	private static String GenerateAreaFileData(String area, List<Pair<String, String>> areaData){
    	    	
    	String header = "timestamp," + area + System.getProperty("line.separator");
    	String values = "";
    	
    	for (Pair<String, String> line : areaData){    		
    		values += line.getValue0() + "," + line.getValue1() + System.getProperty("line.separator");
    	}
    	
    	return header + values;
    }
}
