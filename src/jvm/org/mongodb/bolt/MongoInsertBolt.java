package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.mongodb.StormMongoObjectGrabber;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

//to System.out.println()

public class MongoInsertBolt extends MongoBoltBase {
  static Logger LOG = Logger.getLogger(MongoInsertBolt.class);
  private LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>(10000);
  private MongoBoltTask task;
  private Thread writeThread;
  private boolean inThread;
  private int tuplenum;

    double dxkp00, dxkp10, dPkp00, dPkp01, dPkp10, dPkp11;

  private long start, elapsedTime;

  public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
	super(url, collectionName, mapper, writeConcern);
	LOG.info("bolt creation insert:");
	LOG.info("bolt creation insert end:");
      tuplenum=0;
  }

  public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern, boolean inThread) {
    super(url, collectionName, mapper, writeConcern);
    // Run the insert in a seperate thread
    this.inThread = inThread;
	LOG.info("bolt creation inThread");
      tuplenum=0;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      super.prepare(map, topologyContext, outputCollector);
      // If we want to run the inserts in a separate thread
      if (this.inThread) {
          // Create a task
          this.task = new MongoBoltTask(this.queue, this.mongo, this.db, this.collection, this.mapper, this.writeConcern)
          {
              @Override
              public void execute(Tuple tuple) {
                  // Build a basic object
                  DBObject object = new BasicDBObject();
                  // Map and save the object
                  this.collection.insert(this.mapper.map(object, tuple), this.writeConcern);
              }
          };
          // Run the writeThread
          this.writeThread = new Thread(this.task);
          this.writeThread.start();
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
	///////////////////////Just checking the tuple contents ////////
	Tuple tupledoubled;
    System.out.println("execution bolt:\n");
	//////////////////////////////////////////////
	/////////////////Extract the data//////////////
      //  DBObject object = this.mapper.map(new BasicDBObject(), tuple);
      // DBObject object = new BasicDBObject();
          // Map and save the object


          DBObject objecttostore=this.mapper.map(new BasicDBObject(), tuple);
          dxkp00+=(Double)objecttostore.get("dxkp00");
          dxkp10+=(Double)objecttostore.get("dxkp10");
          dPkp00+=(Double)objecttostore.get("dPkp00");
          dPkp01+=(Double)objecttostore.get("dPkp01");
          dPkp10+=(Double)objecttostore.get("dPkp10");
          dPkp11+=(Double)objecttostore.get("dPkp11");

          System.out.println("Objectoinsert:\n");

          System.out.println("tuplenum num: "+tuplenum+"\n");
          if(tuplenum==0){
              //start = System.nanoTime();
              start = System.currentTimeMillis();

          }else if(tuplenum==100){
              elapsedTime = System.currentTimeMillis()- start;
              float elapsedTimeMin = elapsedTime/(60*1000F);
              float elapsedTimeSec = elapsedTime/(1000F);

              System.out.print("Time to compute a billion tuples: "+elapsedTimeMin+"\n");
              System.out.print("Time to compute a billion tuples: "+elapsedTimeSec+"\n");

          }

          tuplenum++;
          if(tuplenum%4==0){
              dxkp00/=4.0;
              dxkp10/=4.0;
              dPkp00/=4.0;
              dPkp01/=4.0;
              dPkp10/=4.0;
              dPkp11/=4.0;

              objecttostore=BasicDBObjectBuilder.start()
                      .add("dxkp00", dxkp00)
                      .add("dxkp01", dxkp10)
                      .add("dxkp10", dPkp00)
                      .add("dxkp11", dPkp01)
                      .add("dxkp00", dPkp10)
                      .add("dxkp00", dPkp11)
                      .get();
              System.out.println("Object insertion:\n");

              this.collection.insert(objecttostore, this.writeConcern);
              dxkp00=0.0;
              dxkp10=0.0;
              dPkp00=0.0;
              dPkp01=0.0;
              dPkp10=0.0;
              dPkp11=0.0;
          }




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
  }
	
}
