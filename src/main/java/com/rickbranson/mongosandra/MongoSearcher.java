package com.rickbranson.mongosandra;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Set;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;


public class MongoSearcher extends SecondaryIndexSearcher
{
  private static final Logger logger = LoggerFactory.getLogger(MongoSearcher.class);
  private DBCollection coll;

  public MongoSearcher(DBCollection c, SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
  {
    super(indexManager, columns);
    coll = c;
  }

  // Converts IndexOperators to MongoDB moon-language operators
  private String getMongoOperationForIndexOperator(IndexOperator op)
  {
    switch (op) {
      case GTE:
        return "$gte";
      case GT:
        return "$gt";
      case LTE:
        return "$lte";
      case LT:
        return "$lt";
    }

    return null;
  }

  // Deletes a document if it's expired, returning true if that's the case
  private boolean expireDocument(DBObject obj)
  {
    if (!obj.containsKey("t"))
    {
      return false;
    }

    Integer ttl = (Integer)obj.get("t");

    if ((int)(System.currentTimeMillis() / 1000) > ttl.intValue())
    {
      coll.remove(obj);
      return true; 
    }

    return false;
  }

  @Override
  public List<Row> search(IndexClause clause, AbstractBounds range, IFilter dataFilter)
  {
    HashMap<byte[], Row> rows = new HashMap<byte[], Row>();
    BasicDBObject query       = new BasicDBObject();
    BasicDBList valExpr       = new BasicDBList(); 

    // Setup a query, first by making sure the row key is greater than the start_key

    // Create a MongoDB query expression 1-for-1 for Cassandra IndexExpressions
    for (IndexExpression expr : clause.expressions)
    {
      Object mongoOper;
      byte[] exprValue = ByteBufferUtil.getArray(expr.value);
      
      // equality operations are "special" as they don't require a moon language operator
      if (expr.op == IndexOperator.EQ) {
        mongoOper = exprValue; 
      }
      else {
        mongoOper = new BasicDBObject(getMongoOperationForIndexOperator(expr.op), exprValue);
      }

      // The expression always applies to the column value
      BasicDBObject subExpr = new BasicDBObject();

      subExpr.put("v", mongoOper);
      subExpr.put("c", ByteBufferUtil.getArray(expr.column_name));

      valExpr.add(subExpr);
    }
 
    logger.debug("Start key for expression is " + ByteBufferUtil.bytesToHex(clause.start_key));

    // Combines the above expressions together into an "AND" query

    query.put("$or", valExpr);
    //query.put("r", new BasicDBObject("$gt", ByteBufferUtil.getArray(clause.start_key)));

    logger.debug("Mongosandra Query: " + query.toString());

    // Use the "count" from our IndexClause to limit the number of return values
    DBCursor cursor         = coll.find(query);
    QueryPath path          = new QueryPath(baseCfs.columnFamily);
    ByteBuffer startKey     = clause.start_key;
    ByteBuffer lastDataKey  = null;

outer:
    while (cursor.hasNext() && rows.size() < clause.count)
    {
      DBObject doc = cursor.next();

      logger.debug("Got document for query: " + doc.toString());

      if (!expireDocument(doc)) {
        byte[] rowKey     = (byte[])doc.get("r");
        ByteBuffer rkBuf  = ByteBuffer.wrap(rowKey);

        if (!rows.containsKey(rowKey)) {
          DecoratedKey dk = baseCfs.partitioner.decorateKey(rkBuf);

          if (!range.contains(dk.token) || rkBuf.equals(lastDataKey))
            continue;

          ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, path, dataFilter));
          rows.put(rowKey, new Row(dk, data));
        }

        lastDataKey = startKey = rkBuf;
      }
    }

    List<Row> ret = new LinkedList<Row>(rows.values());

    logger.debug("Returning " + Integer.toString(ret.size()) + "rows");

    return ret; 
  }
}
