package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import org.mongodb.StormTupleExtractor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class BoltDtConverter extends BoltDtConverterBase {
  static Logger LOG = Logger.getLogger(BoltDtConverter.class);
  private LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>(10000);
  private MongoBoltTask task;
  private Thread writeThread;
  private boolean inThread;
  public BoltDtConverter( StormTupleExtractor mapper) {
	super( mapper);
	LOG.info("bolt creation dataConverter:");
  }

  public BoltDtConverter( StormTupleExtractor mapper, boolean inThread) {
    super( mapper);
    // Run the insert in a seperate thread
    this.inThread = inThread;
    LOG.info("bolt creation dataconverter inThread");
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);
    // If we want to run the inserts in a separate thread
    if (this.inThread) {
    }
  }

  @Override
  public void execute(Tuple tuple) {
    if (this.inThread) {
	//try if this is where the updates have to be done
	LOG.info("execution bolt:");
	Tuple tupledoubled;
      this.queue.add(tuple);
    } else {
      try {
	LOG.info("execution bolt map:");
	////////////////////code to convert and emit tuples to DataProcessorBolt

	List<Object> listinfo = this.mapper.map(tuple);
	if(listinfo==null){
        System.out.print("listinfo is null\n");

    }else {

        System.out.print("listinfo is not null\n");
        System.out.print("listinfo first: "+listinfo.get(0) +"\n");
        System.out.print("listinfo first: "+listinfo.get(1) +"\n");
        System.out.print("listinfo first: "+listinfo.get(2) +"\n");

       // System.out.print("listinfo first: "+listinfo.get(1) +"\n");

    }

	this.outputCollector.emit(listinfo );
	
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // Execute after insert action
    this.afterExecuteTuple(tuple);
  }

  @Override
  public void afterExecuteTuple(Tuple tuple) {
  }

  @Override
  public void cleanup() {
    if (this.inThread) this.task.stopThread();
    this.mongo.close();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	outputFieldsDeclarer.declare(new Fields(this.mapper.fields()));
  }
}
