package com.music.mongo;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.http.fb.FBResponse;
import com.http.fb.FQLRequestObject;
import com.http.fb.SuccessObject;
import com.http.fb.utils.Constants;
import com.http.fb.utils.UtilFunctions;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoConnector {
	public static void main(String [] args) throws UnknownHostException, UnsupportedEncodingException{
		MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );
		DB db = mongoClient.getDB("Streams");
		db.authenticate("root","passwd".toCharArray());	
		Set<String> colls = db.getCollectionNames();
		
		for (String s : colls) {
		    System.out.println(s);
		}
		
		DBCollection table = db.getCollection("CurrentChannels");
		//Connection.addNewChannelCollection(table, "123", "channelImg", "channelName");
		
		String accessToken = "CAACEdEose0cBACytXA6y4DeRIQ0hWVQ8qjXwWIiOZCPGuu483apZCFQrqBcefEC6Ucfwnh9OrEC9mGUaD3bWcQitt57YGq2l6uWzrLxS1TVqvNxGWzXacKQm8kIed1OULLGWF0elwUUPZBhtfNtZCjxts4ar9Dy1NT7XF5jpFbDATB6upwiU1ZBEreelQVEgDpCcyQ4TM1QZDZD";
		MongoConnector.getFacebookFriendsWithActiveChannels(table, accessToken);

		//String [] ids = {"123","1234"};
		//ArrayList<DBObject>  dbObjects = Connection.getActiveChannels(table, ids);
		
		//for(DBObject obj:dbObjects){
		//	System.out.println(obj.toString());
		//}
	}
	
	public static ArrayList<DBObject> getFacebookFriendsWithActiveChannels(DBCollection table, String accessToken) throws UnsupportedEncodingException{
	
		FBResponse resp = FQLRequestObject.getAppUsers(accessToken, UtilFunctions.encodeURI(Constants.fqlGetAppUsers));
		if(resp.getSuccess() != null) {
			ArrayList<DBObject>  dbObjects = new ArrayList<DBObject>();
			SuccessObject success = resp.getSuccess();
			JSONArray responseArr = (JSONArray) success.getData();	
			for(int i=0;i<responseArr.size();i++){
				JSONObject jsonObj = (JSONObject) responseArr.get(i);
				dbObjects.add(MongoConnector.getAvailableChannels(table, jsonObj.get("uid")+""));				
			}
			return dbObjects;
		}
		return null;
	}
	
	public static DB connectToMongo(String dbName) throws UnknownHostException{		
		MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );
		DB db = mongoClient.getDB(dbName);
		db.authenticate("root","passwd".toCharArray());	
		return db;
	}
	
	public static void removeFromChannelsList(DBCollection table, String fbid){
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("fbid", fbid);
		table.remove(searchQuery);
	}
	
	public static void addEntryToChannel(DBCollection table, String fbid, String channelImg, 
				String channelName, String channelType){
		BasicDBObject document = new BasicDBObject();
		document.put("fbid", fbid);
		document.put("channelName", channelName);
		document.put("channelImg", channelImg);
		document.put("createdDate", new Date());
		document.put("channelType", channelType);
		table.insert(document);	
	}
	
	public static DBObject getAvailableChannels(DBCollection table, String fbid){
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("fbid", fbid);	 
		DBCursor cursor = table.find(searchQuery);
		while (cursor.hasNext()) {
			return cursor.next();
		}
		return null;
	}
}
