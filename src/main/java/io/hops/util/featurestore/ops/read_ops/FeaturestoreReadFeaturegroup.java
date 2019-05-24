package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Map;

/**
 * Builder class for Read-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeaturegroup extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to read
   */
  public FeaturestoreReadFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Gets a featuregroup from a particular featurestore
   *
   * @return a spark dataframe with the featuregroup
   */
  public Dataset<Row> read() {

    if(hudi){
      // returns spark dataframe from a hudi featuregroup
      return  FeaturestoreHelper.getHudiFeaturegroup(spark, name, featurestore, version, hudiArgs, hudiTableBasePath);
    } else {
      // returns spark dataframe from a featuregroup
      return FeaturestoreHelper.getFeaturegroup(spark, name, featurestore, version);
    }

  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreReadFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreReadFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }

  public FeaturestoreReadFeaturegroup setHudi(boolean hudi) {
    this.hudi = hudi;
    return this;
  }

  public FeaturestoreReadFeaturegroup setHudiArgs(Map<String, String> hudiArgs) {
    this.hudiArgs = hudiArgs;
    return this;
  }

  public FeaturestoreReadFeaturegroup setHudiTableBasePath(String hudiTableBasePath) {
    this.hudiTableBasePath = hudiTableBasePath;
    return this;
  }


  
}
