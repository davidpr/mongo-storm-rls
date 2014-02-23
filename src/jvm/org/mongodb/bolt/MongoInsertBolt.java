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

    double x, y, v;

  private long start, elapsedTime;

  public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern) {
	super(url, collectionName, mapper, writeConcern);
	///LOG.info("bolt creation insert:");
	///LOG.info("bolt creation insert end:");
      tuplenum=0;
  }

  public MongoInsertBolt(String url, String collectionName, StormMongoObjectGrabber mapper, WriteConcern writeConcern, boolean inThread) {
    super(url, collectionName, mapper, writeConcern);
    // Run the insert in a seperate thread
    this.inThread = inThread;
	///LOG.info("bolt creation inThread");
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
	///LOG.info("execution bolt:");
	Tuple tupledoubled;
      this.queue.add(tuple);
    } else {
      try {
	///LOG.info("execution bolt map:");
	///////////////////////Just checking the tuple contents ////////
	Tuple tupledoubled;
    ///System.out.println("execution bolt:\n");
	//////////////////////////////////////////////
	/////////////////Extract the data//////////////
      //  DBObject object = this.mapper.map(new BasicDBObject(), tuple);
      // DBObject object = new BasicDBObject();
          // Map and save the object

          DBObject objecttostore=this.mapper.map(new BasicDBObject(), tuple);
          x+=(Double)objecttostore.get("X");
          y+=(Double)objecttostore.get("Y");
          v+=(Double)objecttostore.get("V");

          ///System.out.println("Objectoinsert:\n");

          ///System.out.println("tuplenum num: "+tuplenum+"\n");
          if(tuplenum==0){
              //start = System.nanoTime();
            /////  start = System.currentTimeMillis();

          }else if(tuplenum==100){
           /*   elapsedTime = System.currentTimeMillis()- start;
              float elapsedTimeMin = elapsedTime/(60*1000F);
              float elapsedTimeSec = elapsedTime/(1000F);

              System.out.print("Time to compute a billion tuples: "+elapsedTimeMin+"\n");
              System.out.print("Time to compute a billion tuples: "+elapsedTimeSec+"\n");

              start = System.currentTimeMillis(); */
          }
          tuplenum++;
          if(tuplenum%4==0){
              x/=4.0;
              y/=4.0;
              v/=4.0;

              objecttostore=BasicDBObjectBuilder.start()
                      .add("X", x)
                      .add("Y", y)
                      .add("V", v)
                      .get();

              this.collection.insert(objecttostore, this.writeConcern);
              System.out.print("tuple inserted: " +tuplenum+ "\n");
              x=0.0;
              y=0.0;
              v=0.0;
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
