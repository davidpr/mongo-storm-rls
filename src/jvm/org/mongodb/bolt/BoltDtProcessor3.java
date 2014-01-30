package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.log4j.Logger;
import org.mongodb.StormTupleExtractor;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

//import com.mongodb.BasicDBObjectBuilder;

public class BoltDtProcessor3 extends BoltDtProcessorBase3 implements Serializable{//
  static Logger LOG = Logger.getLogger(BoltDtProcessor3.class);
  private LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>(10000);
  private MongoBoltTask task;
  private Thread writeThread;
  private boolean inThread;

  private WriteConcern writeConcern;
  //davidp 
  // RealMatrix[][] xkpM;// {{0.0616962065187},{1.0}};
  private double dPk,dPkp,dxkp00, dxkp10;
  //double xkp;
  private RealMatrix Pk, Pkp;
  private RealMatrix xk, xkp, Hk, Hkt, Rk, yk, kk, tmp1, ide;

  private double [][] xkpres;
  private double [][] Pkpres;
  
  private boolean sentinel;
  private double numbolts;
  private boolean dretreived;
  private boolean firsttime;

  private boolean firstuple;
  private String collectionNameProcessor;

    private int number;

  public BoltDtProcessor3(String url, String dbName, String collectionNameProcessor, StormTupleExtractor mapper,WriteConcern writeConcern) {
	super(url, null, collectionNameProcessor, null, mapper, writeConcern);
	//(url,null, collectionNameProcessor, mapper2,writeConcern0.NONE);//from topology
	//super(url, null, new String[]{collectionName}, null, mapper);from mongocappedcollectionspout
	//super(url, null, new String[]{collectionName}, null);
	LOG.info("bolt processor creation before retreival:");
    System.out.print("connection to database :"+ collectionNameProcessor +"\n");
	LOG.info("bolt processor creation insert before while:");
    this.collectionNameProcessor=collectionNameProcessor;
    this.firstuple=true;
    restoreState();
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector ) {
    super.prepare(map, topologyContext, outputCollector);
    // If we want to run the inserts in a separate thread
  }

  @Override
  public void execute(Tuple tuple) {
      if (this.inThread) {
          //try if this is where the updates have to be done
          LOG.info("execution bolt:");
          Tuple tupledoubled;
          this.queue.add(tuple);
      } else {
          ///////////////PROCESS TUPLES//////////////////////
          try {
              LOG.info("execution bolt map:");
              ////////////////////code to convert and emit tuples to DataProcessorBolt
              List<Object> listinfo = this.mapper.map(tuple);
              ///////////////////////////////////////////
              System.out.println("hiformprocessor3\n");
              double X=(Double)listinfo.get(0);
              double Y=(Double)listinfo.get(1);
              double V=(Double)listinfo.get(2);
              System.out.println("contents-> X:Y:V : "+X+" "+Y+" "+V+" \n");
              //////////////kk//////////////
              //double[][] matrixData2 = { {0.0616962065187d}, {1.0d}};
              //xkpM = new Array2DRowRealMatrix(matrixData2);
              //double[][] Hkdata ={{X},{1.0d}};
              double[][] Hkdata ={{X,1.0d}};
              Hk = new Array2DRowRealMatrix(Hkdata);
              Hkt=Hk.transpose();
              System.out.println("after transpose\n");
              double[][] Rkdata ={{V}};
              Rk = new Array2DRowRealMatrix(Rkdata);
              //computations
              //tmp1=Hkt.multiply(Pkp);->need to multiply by a scalar
              tmp1=Hk.multiply(Pkp);//sc
              double[][]dHk=Hk.getData();
              mmult(dHk,"Hk");
              double[][]result=tmp1.getData();
              mmult(result, "tmp1");
              double dHkt[][]=Hkt.getData();
              mmult(dHkt,"Hkt");
              tmp1=tmp1.multiply(Hkt);
              double result2[][]=tmp1.getData();
              mmult(result2,"tmp2");
              double[][]dRk=Rk.getData();
              mmult(dRk, "Rk");
              tmp1=tmp1.add(Rk);
              //tmp1=tmp1.getInverse();not working
              tmp1  = new LUDecomposition(tmp1).getSolver().getInverse();//LUDecompositionImpl not located
              tmp1=Hkt.multiply(tmp1);
              System.out.println("before kk\n");
              kk=Pkp.multiply(tmp1);//sc
              System.out.println("after kk\n");
              //////////////xk///////////////////
             /*t1=Hk*xkp
                t2=yk-t1
                t3=Kk*t2
                xk=xkp+t3#xk!!	*/
              double[][] ykdata={{Y}};
              yk = new Array2DRowRealMatrix(ykdata);
              //computations
              tmp1=Hk.multiply(xkp);
              tmp1=yk.subtract(tmp1);
              tmp1=kk.multiply(tmp1);
              xk=xkp.add(tmp1);
              ///////////////Pk//////////////////
            /*t1=Kk*Hk
            ide=np.identity(2)
            ide=np.matrix(ide)
            t2=ide-t1
            Pk=t2*Pkp#Pk!*/
              double[][] idedata={{1.0d,0.0d},{0.0d,1.0d}};
              ide = new  Array2DRowRealMatrix(idedata);
              //computations
              tmp1=kk.multiply(Hk);
              tmp1= ide.subtract(tmp1);
              Pk=tmp1.multiply(Pkp);//sc
              System.out.println("after P\n");
              ///////////////////////////////////////
              Pkp=Pk;
              xkp=xk;
              LOG.info("xk"+xk);
              xkpres=xk.getData();
              listinfo.add(xkpres[0][0]);//to be removed
              listinfo.add(xkpres[1][0]);//to be removed
              //////////print the results////////////
              LOG.info("xk00"+xkpres[0][0]);
              LOG.info("xk10"+xkpres[1][0]);
              //////////////////////////////////////////

              List<Object> listtoinsertbolt = new ArrayList<Object>();
              listtoinsertbolt.add(xkpres[0][0]);
              listtoinsertbolt.add(xkpres[1][0]);
              //
              Pkpres=Pkp.getData();
              listtoinsertbolt.add(Pkpres[0][0]);
              listtoinsertbolt.add(Pkpres[0][1]);
              listtoinsertbolt.add(Pkpres[1][0]);
              listtoinsertbolt.add(Pkpres[1][1]);





              this.outputCollector.emit(listtoinsertbolt);
          } catch (Exception e) {
              e.printStackTrace();
          }
      }
      // Execute after insert action
      this.afterExecuteTuple(tuple);
  }

    private void restoreState() {
        System.out.print("restoring state\n");
        ////////DB
        Mongo mongo;
        DB db;
        DBCollection collection;
        try {
            MongoURI uri = new MongoURI(url);
            // Open the db
            mongo = new Mongo(uri);
            // Grab the db
            db = mongo.getDB(uri.getDatabase());
            // If we need to authenticate do it
            if (uri.getUsername() != null) {
                db.authenticate(uri.getUsername(), uri.getPassword());
            }
            // Grab the collection from the uri
            collection = db.getCollection(collectionName);
        } catch (UnknownHostException e) {
            // Die fast
            throw new RuntimeException(e);
        }

        collection = db.getCollection(this.collectionNameProcessor);
        DBCursor cursor = collection.find(query)
                .sort(new BasicDBObject("timestamp", -1));

        if (cursor.hasNext()) {//check out if this is the first itme we start the topology
            System.out.print("cursor has next\n");
            DBObject object = cursor.next();
            System.out.print("cursor next\n");

            if (object.containsField("timestamp")) {//now get the last state values
                LOG.info("there's  timestamp dxkp00 in the document:");
                //retreive the data
                double dxkp00 = (Double) object.get("dxkp00");
                double dxkp10 = (Double) object.get("dxkp10");
                double dPkp00 = (Double) object.get("dPkp00");
                double dPkp01 = (Double) object.get("dPkp01");
                double dPkp10 = (Double) object.get("dPkp10");
                double dPkp11 = (Double) object.get("dPkp11");

                System.out.print("Object values: " + dxkp00 + dxkp10 + dPkp00 + dPkp01 + dPkp10 + dPkp11 + "\n");

                double[][] Pkdata = {{dPkp00, dPkp01}, {dPkp10, dPkp11}};
                double[][] Pkpdata = {{dPkp00, dPkp01}, {dPkp10, dPkp11}};
                double[][] xkpdata = {{dxkp00}, {dxkp10}};

                this.xkp = new Array2DRowRealMatrix(xkpdata);
                this.Pkp = new Array2DRowRealMatrix(Pkpdata);
                this.Pk = new Array2DRowRealMatrix(Pkdata);
            } else {//something was badly stored
                LOG.info("there's no dxkp00 in the collection but there are documents:");
                this.dPk = 2000.0d;
                this.dPkp = 2000.0d;
                double[][] Pkdata = {{2000.0d, 0.0d}, {0.0d, 200.0d}};
                double[][] Pkpdata = {{2000.0d, 0.0d}, {0.0d, 200.0d}};
                double[][] xkpdata = {{0.0616962065187d}, {1.0d}};
                this.xkp = new Array2DRowRealMatrix(xkpdata);
                this.Pkp = new Array2DRowRealMatrix(Pkpdata);
                this.Pk = new Array2DRowRealMatrix(Pkdata);
            }
        } else {//first time we stream
            LOG.info("there's no document in the collection:");
            this.dPk = 2000.0d;
            this.dPkp = 2000.0d;
            double[][] Pkdata = {{2000.0d, 0.0d}, {0.0d, 200.0d}};
            double[][] Pkpdata = {{2000.0d, 0.0d}, {0.0d, 200.0d}};
            double[][] xkpdata = {{0.0616962065187d}, {1.0d}};
            this.xkp = new Array2DRowRealMatrix(xkpdata);
            this.Pkp = new Array2DRowRealMatrix(Pkpdata);
            this.Pk = new Array2DRowRealMatrix(Pkdata);
        }
    }

    @Override
  public void afterExecuteTuple(Tuple tuple) {}

  @Override
  public void cleanup() {
    if (this.inThread) this.task.stopThread();}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	outputFieldsDeclarer.declare(new Fields(this.mapper.fields()));}
  
  private void mmult(double [][]mm, String name){
	System.out.print("name: "+name+"\n");
		for (int i = 0; i < mm.length; i++) {
                 	for (int j = 0; j < mm[i].length; j++) {
                        System.out.print(mm[i][j] + " (" + i +") ("+ j +") ");
                 }
                 System.out.print("\n");
         	}

	}
  
}
