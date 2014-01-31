package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.mongodb.StormTupleExtractor;

import java.util.Map;

public abstract class BoltDtProcessorBase3 extends BaseRichBolt {
  static Logger LOG = Logger.getLogger(BoltDtProcessorBase3.class);
  protected static StormTupleExtractor wholeDocumentMapper = null;	//mongospoutbase	   

    // Bolt runtime objects
    protected Map map;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;

    // Constructor arguments
    protected static String url;
    protected static String collectionName;
    protected StormTupleExtractor mapper;
    protected WriteConcern writeConcern;

    static DBObject query;

  public BoltDtProcessorBase3(String url, String dbName, String collectionName, DBObject query, StormTupleExtractor mapper, WriteConcern writeConcern){
      BoltDtProcessorBase3.url = url;
      BoltDtProcessorBase3.query = query;
      BoltDtProcessorBase3.collectionName = collectionName;
      this.mapper = mapper;
      this.writeConcern = writeConcern == null ? WriteConcern.NONE : writeConcern;
	///LOG.info("bolt creation processor base:\n ");
	System.out.println("execution bolt processor base : \n");
  }

   @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
       this.map = map;
       this.topologyContext = topologyContext;
       this.outputCollector = outputCollector;
      /// LOG.info("prepare base processor: \n");
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
