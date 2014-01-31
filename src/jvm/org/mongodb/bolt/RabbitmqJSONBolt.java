package org.mongodb.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class RabbitmqJSONBolt extends BaseRichBolt 
{
    private OutputCollector collector;

    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) 
    {
        this.collector = collector;
    }

    public void execute( Tuple tuple ) 
    {

	Fields fields = tuple.getFields();
    	int numFields = fields.size();
 	///System.out.println("numer of fields: " + numFields + " \n");

    	for (int idx = 0; idx < numFields; idx++) {
        	String name = fields.get(idx);
        	Object value = tuple.getValue(idx);
		///System.out.println("Field name: " + name +", Field value: " + value +" \n");
    	}

        collector.ack( tuple );
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer ) 
    {
        declarer.declare( new Fields( "message" ));
    }   
    
}
