package com.rickbranson.mongosandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LocalByPartionerType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;

import java.net.UnknownHostException;

public class MongoIndex extends PerColumnSecondaryIndex
{
  private static final String MONGO_DATABASE_NAME = "mongosandra";
  private static final Logger logger              = LoggerFactory.getLogger(MongoIndex.class);
  private ColumnDefinition columnDef;
  private Mongo m;
  private DB db;
  private DBCollection coll;

  public MongoIndex()
  {
  }

  public void init()
  {
    columnDef = columnDefs.iterator().next();

    try {
      m = new Mongo();  
    }
    catch (UnknownHostException ex) { /* unknown localhost, really? */ }

    db    = m.getDB(MONGO_DATABASE_NAME);
    coll  = db.createCollection(columnDef.getIndexName(), null);

    ensureIndex(coll);
  }

  private void ensureIndex(DBCollection coll)
  {
    BasicDBObject indexPredicate = new BasicDBObject();

    indexPredicate.put("c", 1);
    indexPredicate.put("r", 2);
    indexPredicate.put("v", 3);

    coll.ensureIndex(indexPredicate);
  }

  private BasicDBObject objectForRow(ByteBuffer rowKey)
  {
    BasicDBObject document = new BasicDBObject();
    document.put("r", ByteBufferUtil.getArray(rowKey));
    return document;
  }

  private BasicDBObject objectForColumnValue(DecoratedKey<?> valueKey, ByteBuffer rowKey)
  {
    BasicDBObject document = objectForRow(rowKey);
    document.put("v", ByteBufferUtil.getArray(valueKey.key));
    return document;
  }

  private BasicDBObject objectForColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col)
  {
    BasicDBObject document = objectForColumnValue(valueKey, rowKey);
    document.put("c", ByteBufferUtil.getArray(col.name()));
    return document;
  }

  private BasicDBObject objectForColumnName(ByteBuffer columnName)
  {
    BasicDBObject document = new BasicDBObject();
    document.put("c", ByteBufferUtil.getArray(columnName));
    return document;
  }

  @Override
  public void deleteColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col)
  {
    coll.findAndRemove(objectForColumn(valueKey, rowKey, col));
  }

  @Override
  public void insertColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col)
  {
    BasicDBObject doc = objectForColumn(valueKey, rowKey, col);
    coll.findAndRemove(doc);

    if (col instanceof ExpiringColumn)
    {
      ExpiringColumn ec = (ExpiringColumn)col;
      doc.put("t", ec.getTimeToLive());
    }
    
    coll.insert(doc);
  }

  @Override
  public void updateColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col)
  {
    insertColumn(valueKey, rowKey, col);
  }

  @Override
  public void removeIndex(ByteBuffer columnName) throws IOException
  {
    coll.findAndRemove(objectForColumnName(columnName));
  }

  @Override
  public void forceBlockingFlush() throws IOException
  {
  }

  @Override
  public void unregisterMbean()
  {
  }

  @Override
  public MongoSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
  {
    return new MongoSearcher(coll, baseCfs.indexManager, columns);
  }

  @Override
  public String getIndexName()
  {
    return coll.getFullName();
  }

  @Override
  public void renameIndex(String newCfName) throws IOException
  {
    try 
    {
      coll.rename(newCfName);
    }
    catch (MongoException e)
    {
      throw new IOException("MongoException when renaming index: " + e.toString());
    }
  }

  @Override
  public ColumnFamilyStore getUnderlyingCfs()
  {
    return null; // We don't use CFs to store the index
  }

  @Override
  public void validateOptions() throws ConfigurationException
  {
    // no options
  }
}
