package org.mongodb;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.rapportive.storm.amqp.QueueDeclaration;
import com.rapportive.storm.amqp.SharedQueueWithBinding;
import com.rapportive.storm.scheme.SimpleJSONScheme;
import com.rapportive.storm.spout.AMQPSpout;
import org.mongodb.bolt.BoltDtConverter;
import org.mongodb.bolt.BoltDtProcessor3;
import org.mongodb.bolt.MongoInsertBolt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//import org.json.JSONObject;

//importing the Scheme class
/////rabbitmq, amqp and amqpspout includes
//import com.rapportive.storm.amqp.HAPolicy;
////////////////////////////////////////////////

//import org.apache.log4j.Logger;

public class MongoTopology
{
    public static void main(String[] args) throws Exception 
    {
    TopologyBuilder builder = new TopologyBuilder();
    String url = "mongodb://127.0.0.1:27017/regression";

	QueueDeclaration qd = new SharedQueueWithBinding("stormqueue", "stormexchange", "stormkey");
	//next calls are in SetupAMQP method of AMQPSpout class
	//ConnectionFactory factory = new ConnectionFactory();
	//factory.setHost("localhost");
	//Connection connection = factory.newConnection();
	//Channel channel = connection.createChannel();
	Scheme scheme = new SimpleJSONScheme();

    builder.setSpout( "spout", new AMQPSpout("127.0.0.1", 5672, "guest", "guest", "/", qd, scheme));

    /*builder.setBolt( "prime", new RabbitmqJSONBolt() )
           .shuffleGrouping("spout");     */

    ////////////////////////////////////////////////////////////////
    //////////////////BOLT DATA CONVERTER///////////////////////////
    ////////////////////////////////////////////////////////////////
    StormTupleExtractor mapper1 = new StormTupleExtractor() {//field mapper
        @Override
        public List<Object> map(Tuple tuple) {
            List<Object> listobj = new ArrayList<Object>();
            if(tuple.contains ("message")){
                ///System.out.print("tuple contained message field\n");
                ///System.out.print(tuple.getValueByField("message")+ "\n");
                Object mvalue= tuple.getValueByField("message") ;
                String svalue=mvalue.toString();
                System.out.println("svalue is: " + svalue + "\n");
                /*String[] couple = svalue.split(",");
                for(int i =0; i < couple.length ; i++) {
                    String[] items =couple[i].split(":");
                    //items[0]; //Key
                    //items[1]; //Value
                    System.out.print("items: "+items[1]+"\n");
                    if()}
                */
                //String Data=svalue.getEntity().getText().toString(); // reading the string value
                //JSONObject json;

                JsonParser parser = new JsonParser();
                JsonObject obj = (JsonObject)parser.parse(svalue);
                /*JsonElement id1 = obj.get("X");
                JsonElement id2 = obj.get("Y");
                JsonElement id3 = obj.get("V");  */
                double id1 = obj.get("X").getAsDouble();
                double id2 = obj.get("Y").getAsDouble();
                double id3 = obj.get("V").getAsDouble();

                listobj.add(id1);
                listobj.add(id2);
                listobj.add(id3);

                ///System.out.print("return listobj with x y and v\n");
                return listobj;
            }
            if(tuple.contains("X")){
                ///System.out.print("tuple contained X field\n");

                listobj.add(tuple.getValueByField("X"));
                listobj.add(tuple.getValueByField("Y"));
                listobj.add(tuple.getValueByField("V"));
                return listobj; }
            else{return null;} }
        @Override
        public String[] fields() {//this document allows not having to change declareOutputFields
            return new String[]{"X","Y","V"}; }};
    // Create a mongo data converter bolt
    BoltDtConverter DataConverterBolt = new BoltDtConverter( mapper1);
    // Add it to the builder accepting content from the sum bolt/spout
    builder.setBolt("converter", DataConverterBolt, 1).allGrouping("spout");

    ///////////////////////////////////////////////////////////////
    /////////////////BOLT DATA PROCESSOR///////////////////////////
    ///////////////////////////////////////////////////////////////
    //String collectionNameProcessor = "diabetesstate";//in case of complex state storage
    String collectionNameProcessor = "diabetesoutput";

    StormTupleExtractor mapper2 = new StormTupleExtractor() {//field mapper
        @Override
        public List<Object> map(Tuple tuple) {
            List<Object> listobj = new ArrayList<Object>();
            if(tuple.contains("X")){
                listobj.add(tuple.getValueByField("X"));
                listobj.add(tuple.getValueByField("Y"));
                listobj.add(tuple.getValueByField("V"));
                return listobj; }
            else{return null;} }
        @Override
        public String[] fields() {//this document allows not having to change declareOutputFields
            //return new String[]{"X","Y","V","Xk00","Xk10"};} };
            return new String[]{"dxk00","dxk10","dPkp00","dPkp01","dPkp10","dPkp11"};} };
    // Create a mongo data processor bolt
    WriteConcern writeConcern0=new WriteConcern();
    //BoltDtProcessor DataProcessorBolt = new BoltDtProcessor( mapper2);
    BoltDtProcessor3 DataProcessorBolt = new BoltDtProcessor3(url,null, collectionNameProcessor, mapper2, writeConcern0.NONE);
    // Add it to the builder accepting content from the sum bolt/spout
    builder.setBolt("processor", DataProcessorBolt, 1).allGrouping("converter");
    ////////////////////////////////////////////////////////////////
    //////////////////////////////BOLT DB///////////////////////////
    ////////////////////////////////////////////////////////////////
    // Mongo insert bolt instance
    String collectionNameOutput = "diabetesoutput";
    MongoInsertBolt mongoInserBolt = null;
    StormMongoObjectGrabber mapper3 = new StormMongoObjectGrabber() {//field mapper
        @Override
        public DBObject map(DBObject object, Tuple tuple) {
            return BasicDBObjectBuilder.start()
                    .add("dxkp00", tuple.getValueByField("dxk00"))
                    .add("dxkp10", tuple.getValueByField("dxk10"))
                    .add("dPkp00", tuple.getValueByField("dPkp00"))//added
                    .add("dPkp01", tuple.getValueByField("dPkp01"))//added
                    .add("dPkp10", tuple.getValueByField("dPkp10"))//added
                    .add("dPkp11", tuple.getValueByField("dPkp11"))//added
                            //.add("number", tuple.getValueByField("number"))//added
                    .add("timestamp", new Date())
                    .get(); } };
    WriteConcern writeConcern=new WriteConcern();
    // Create a mongo bolt
    mongoInserBolt = new MongoInsertBolt(url, collectionNameOutput, mapper3, writeConcern.NONE);
    // Add it to the builder accepting content from the sum bolt/spout
    builder.setBolt("mongo", mongoInserBolt, 1).allGrouping("processor");
    /////////////////////////////////////////////////////////////////////////////////

	Config conf = new Config();
	conf.setDebug(true);
	conf.setNumWorkers(1);
	conf.setMaxSpoutPending(5000);

	try{
		StormSubmitter.submitTopology( args[0], conf, builder.createTopology() );
	}
	catch(AlreadyAliveException e){
		
	}
    }
}
