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

public abstract class BoltDtConverterBase extends BaseRichBolt {
  static Logger LOG = Logger.getLogger(BoltDtConverterBase.class);

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

  public BoltDtConverterBase( StormTupleExtractor mapper) {

    this.mapper = mapper;
    
	LOG.info("bolt creation base converter:\n ");
	System.out.println("execution bolt base converter: \n");
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    // Save all the objects from storm
    this.map = map;
    this.topologyContext = topologyContext;
    this.outputCollector = outputCollector;

	LOG.info("prepare base converter: \n");
        System.out.println("prepare  base converter: \n");
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
