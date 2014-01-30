package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.mongodb.StormTupleExtractor;

import java.util.List;

import java.util.ArrayList;//davidp


import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;
import backtype.storm.tuple.Fields;	
import com.mongodb.BasicDBObjectBuilder;

import backtype.storm.tuple.Fields;	
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.analysis.*;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.Date;
import com.mongodb.WriteConcern;
import com.mongodb.BasicDBObjectBuilder;
//import com.mongodb.BasicDBObjectBuilder;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class BoltDtProcessor extends BoltDtProcessorBase implements Serializable{//
  static Logger LOG = Logger.getLogger(BoltDtProcessor.class);
  private LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>(10000);
  private MongoBoltTask task;
  private Thread writeThread;
  private boolean inThread;
  
  private DBCursor cursor;
  private DBObject query;
  private WriteConcern writeConcern;
  //davidp 
  // RealMatrix[][] xkpM;// {{0.0616962065187},{1.0}};
  private double dPk,dPkp,dxkp00, dxkp10;
  private RealMatrix Pk, Pkp;
  private RealMatrix xk, xkp, Hk, Hkt, Rk, yk, kk, tmp1, ide;
  
  private double idrun;
  private double [][] xkpres;
  private double [][] Pkpres;
  private boolean sentinel;
  private double numbolts;
  private boolean dretreived;
  private boolean firsttime;
  private Mongo mongo;

  public BoltDtProcessor( StormTupleExtractor mapper) {
	super( mapper);
	LOG.info("bolt creation processor insert:");
	
	dPk=2000;
	dPkp=2000;
	//xkp=0.0616962065187;
	//double[][] Hkdata ={{X},{1.0d}};
	//Hk = new Array2DRowRealMatrix(Hkdata);
	double[][] Pkdata  ={{2000.0d,0.0d},{0.0d,200.0d}};
	double[][] Pkpdata ={{2000.0d,0.0d},{0.0d,200.0d}};
	double[][] xkpdata ={{0.0616962065187d},{1.0d}};
	//double[][] xkpdata ={{0.0616962065187d,1.0d}};
  	//double[][] xkdata = {{0.0616962065187d}};
	//Pk  = new Array2DRowRealMatrix(Pkdata);
	//Pkp = new Array2DRowRealMatrix(Pkpdata);
	xkp = new Array2DRowRealMatrix(xkpdata);
	Pkp = new Array2DRowRealMatrix(Pkpdata);
	Pk  = new Array2DRowRealMatrix(Pkdata);
	//xk = new Array2DRowRealMatrix(xkdata);
	//xkpM=createMatrix(2,1);
  	//double[][] matrixData = {{0.0616962065187d}, {1.0d}};//doesn't work
	//RealMatrix[][] xkpM = MatrixUtils.createRealMatrix(matrixData);
	//xkpM=xkpM.transpose();

  }
  public BoltDtProcessor(String url, String dbName, String collectionNameProcessor, StormTupleExtractor mapper,WriteConcern writeConcern) {	
	super(url, null,new String[]{collectionNameProcessor}, null, mapper);
	//(url,null, collectionNameProcessor, mapper2,writeConcern0.NONE);//from topology
	//super(url, null, new String[]{collectionName}, null, mapper);from mongocappedcollectionspout
	//super(url, null, new String[]{collectionName}, null);

	LOG.info("bolt processor creation before retreival:");
	this.idrun=0;
	this.writeConcern = writeConcern == null ? WriteConcern.NONE : writeConcern;
	double newidrun=0;

	LOG.info("bolt processor creation insert before while:");
	
	try{
	Thread.sleep(2000);
	}catch(Exception e)
	{System.out.println(e);}

		this.dPk=2000.0d;
		this.dPkp=2000.0d;
		double[][] Pkdata  ={{2000.0d,0.0d},{0.0d,200.0d}};
		double[][] Pkpdata ={{2000.0d,0.0d},{0.0d,200.0d}};
		double[][] xkpdata ={{0.0616962065187d},{1.0d}};
		xkp = new Array2DRowRealMatrix(xkpdata);
		Pkp = new Array2DRowRealMatrix(Pkpdata);
		Pk  = new Array2DRowRealMatrix(Pkdata);
	}

  public BoltDtProcessor( StormTupleExtractor mapper, boolean inThread) {
    super( mapper);
    // Run the insert in a seperate thread
    this.inThread = inThread;
    LOG.info("bolt creation inThread");
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector ) {
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
	///////////////////////////////////////////
	//DBObject object = this.mapper.map(new BasicDBObject(), tuple);
	//String objectstr= object.toString();
	//System.out.println("object contents: "+objectstr+ " \n");
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
	//double [][] Pkpres=Pk.getData(); 
	LOG.info("xk00"+xkpres[0][0]);
	LOG.info("xk10"+xkpres[1][0]);
	
	//////////////////////////////////////////
	//old emit to emit X,Y,V, xkpres[0][1],xkpres[1][0]
	//this.outputCollector.emit(listinfo);//  emit in bolts doesn't support ids (tuple.getMessageId(
	//emit only relevant information to save the state
	//List<Object> listtoinserbolt = this.mapper.map(tuple);
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

  @Override
  public void afterExecuteTuple(Tuple tuple) {
  }

  @Override
  public void cleanup() {
    if (this.inThread) this.task.stopThread();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	outputFieldsDeclarer.declare(new Fields(this.mapper.fields()));
  }
  
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
