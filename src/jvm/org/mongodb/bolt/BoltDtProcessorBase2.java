package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.mongodb.StormMongoObjectGrabber;
import org.mongodb.StormTupleExtractor;

import java.net.UnknownHostException;
import java.util.Map;

import java.io.*;

import org.mongodb.StormTupleExtractor;
//import org.mongodb.MongoObjectGrabber;

public abstract class BoltDtProcessorBase2 extends BaseRichBolt {
  static Logger LOG = Logger.getLogger(BoltDtProcessorBase2.class);
  protected static StormTupleExtractor wholeDocumentMapper = null;	//mongospoutbase	   
    // Bolt runtime objects
    protected Map map;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;
    // Constructor arguments
    protected String url;
    protected String collectionName;
    protected StormTupleExtractor mapper;
    protected WriteConcern writeConcern;
    // Mongo objects
    protected Mongo mongo;
    protected DB db;
    protected DBCollection collection;

  public BoltDtProcessorBase2(String url, String dbName, String collectionName, DBObject query, StormTupleExtractor mapper, WriteConcern writeConcern){
      this.url = url;
      this.collectionName = collectionName;
      this.mapper = mapper;
      this.writeConcern = writeConcern == null ? WriteConcern.NONE : writeConcern;
	LOG.info("bolt creation processor base:\n ");
	System.out.println("execution bolt processor base : \n");

  }

   @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
       this.map = map;
       this.topologyContext = topologyContext;
       this.outputCollector = outputCollector;

       LOG.info("prepare base processor: \n");
       System.out.println("prepare  base processor: \n");
       try {
           MongoURI uri = new MongoURI(this.url);
           // Open the db
           this.mongo = new Mongo(uri);
           // Grab the db
           this.db = this.mongo.getDB(uri.getDatabase());
           // If we need to authenticate do it
           if (uri.getUsername() != null) {
               this.db.authenticate(uri.getUsername(), uri.getPassword());
           }
           // Grab the collection from the uri
           this.collection = this.db.getCollection(this.collectionName);
       } catch (UnknownHostException e) {
           // Die fast
           throw new RuntimeException(e);
       }
       // Attempt to open a db connection
  }
  
  @Override
  public abstract void execute(Tuple tuple);

  /**
   * Lets you handle any additional emission you wish to do
   *
   * @param tuple
   */
  public abstract void afterExecuteTuple(Tuple tuple);

  @Override
  public abstract void cleanup();

  @Override
  public abstract void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);
}
