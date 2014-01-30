package org.mongodb;

import com.mongodb.DBObject;

import java.io.Serializable;
import java.util.List;
import backtype.storm.tuple.Tuple;

public abstract class StormTupleExtractor implements Serializable {

  private static final long serialVersionUID = 7265794696380763567L;

  public abstract List<Object> map(Tuple tuple);

  public abstract String[] fields();

}

