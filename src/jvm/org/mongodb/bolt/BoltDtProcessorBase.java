package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.mongodb.StormTupleExtractor;

import java.net.UnknownHostException;
import java.util.Map;

import java.io.*;

import org.mongodb.StormTupleExtractor;
//import org.mongodb.MongoObjectGrabber;

public abstract class BoltDtProcessorBase extends BaseRichBolt {
  static Logger LOG = Logger.getLogger(BoltDtProcessorBase.class);
  protected static StormTupleExtractor wholeDocumentMapper = null;	//mongospoutbase	   
  // Bolt runtime objects
  protected Map map;
  protected TopologyContext topologyContext;
  protected OutputCollector outputCollector;
  // Constructor arguments
  protected String url;
  protected String collectionName;
  protected StormTupleExtractor mapper;
  // Mongo objects
  protected Mongo mongo;
  protected DB db;
  protected DBCollection collection;
  private BoltDtProcessorTask boltTask;//davidp
  private String dbName;
  private String[] collectionNames;
  private DBObject query;
  private boolean task;

  public BoltDtProcessorBase( StormTupleExtractor mapper) {
    this.mapper = mapper;
    this.task=false;
	LOG.info("bolt creation processor base:\n ");
	System.out.println("execution bolt processor base : \n");
	
  }
  //super(url, dbName, null, mapper);
  //public MongoSpoutBase(String url, String dbName, String[] collectionNames, DBObject query, MongoObjectGrabber mapper) {

  public BoltDtProcessorBase(String url, String dbName, String[] collectionNames, DBObject query, StormTupleExtractor mapper)
  {
    //from MongoSpoutBase 	
    this.url = url;
    this.dbName = dbName;
    this.collectionNames = collectionNames;
    this.query = query;
    this.mapper = mapper == null ? wholeDocumentMapper : mapper;
	//this.mapper = mapper;
	//this.task=true;
	this.task=false;
	LOG.info("bolt creation processor base:\n ");
	System.out.println("execution bolt processor base : \n");
	 try {
      MongoURI uri = new MongoURI(url);
      // Create mongo instance
      mongo = new Mongo(uri);
      // Get the db the user wants
      db = mongo.getDB(dbName == null ? uri.getDatabase() : dbName);
      // If we need to authenticate do it
      if (uri.getUsername() != null) {
        db.authenticate(uri.getUsername(), uri.getPassword());
      }
    } catch (UnknownHostException e) {
      // Log the error
      LOG.error("Unknown host for Mongo DB", e);
      // Die fast
      throw new RuntimeException(e);
    }
    //to remove
	 if(this.task){
	//davidp 
	// Set up an executor
	this.boltTask = new BoltDtProcessorTask( this.url, this.dbName, this.collectionNames, this.query){
	};
	// Start thread
	Thread thread = new Thread(this.boltTask);
	thread.start();
	}
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    // Save all the objects from storm
    this.map = map;
    this.topologyContext = topologyContext;
    this.outputCollector = outputCollector;
    LOG.info("prepare base processor: \n");
    System.out.println("prepare  base processor: \n");
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
