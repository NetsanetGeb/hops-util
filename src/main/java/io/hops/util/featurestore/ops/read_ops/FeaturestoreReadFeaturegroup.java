package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Constants;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreJdbcConnectorDTO;
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
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public Dataset<Row> read() throws HiveNotEnabled, FeaturegroupDoesNotExistError,
      StorageConnectorDoesNotExistError {
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
        name, version);
    getSpark().sparkContext().setJobGroup("Fetching Feature Group",
        "Getting Feature group: " + name + " from the featurestore:" + featurestore, true);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP){
      return readOnDemandFeaturegroup((OnDemandFeaturegroupDTO) featuregroupDTO, featurestoreMetadata);
    } else {
      return readCachedFeaturegroup();
    }
  }

  /**
   * Gets an on-demand featuregroup from a particular featurestore
   *
   * @param onDemandFeaturegroupDTO featuregroup metadata
   * @param featurestoreMetadataDTO featurestore metadata
   * @return a spark dataframe with the featuregroup
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public Dataset<Row> readOnDemandFeaturegroup(
      OnDemandFeaturegroupDTO onDemandFeaturegroupDTO, FeaturestoreMetadataDTO featurestoreMetadataDTO)
      throws HiveNotEnabled, StorageConnectorDoesNotExistError {
    FeaturestoreJdbcConnectorDTO jdbcConnector = (FeaturestoreJdbcConnectorDTO) FeaturestoreHelper.findStorageConnector(
        featurestoreMetadataDTO.getStorageConnectors(), onDemandFeaturegroupDTO.getJdbcConnectorName());
    String jdbcConnectionString = jdbcConnector.getConnectionString();
    String[] jdbcConnectionStringArguments = jdbcConnector.getArguments().split(",");

    // Substitute jdbc arguments in the connection string
    for (int i = 0; i < jdbcConnectionStringArguments.length; i++) {
      if (jdbcArguments != null && jdbcArguments.containsKey(jdbcConnectionStringArguments[i])) {
        jdbcConnectionString = jdbcConnectionString + jdbcConnectionStringArguments[i] +
            Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + jdbcArguments.get(jdbcConnectionStringArguments[i]) +
            Constants.JDBC_CONNECTION_STRING_DELIMITER;
      } else {
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_TRUSTSTORE_ARG)){
          String trustStore = Hops.getTrustStore();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_TRUSTSTORE_ARG +
              Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + trustStore +
              Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_TRUSTSTORE_PW_ARG)){
          String pw = Hops.getKeystorePwd();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_TRUSTSTORE_PW_ARG +
              Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + pw +
              Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_KEYSTORE_ARG)){
          String keyStore = Hops.getKeyStore();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_KEYSTORE_ARG +
              Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + keyStore +
              Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_KEYSTORE_PW_ARG)){
          String pw = Hops.getKeystorePwd();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_KEYSTORE_PW_ARG +
              Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + pw +
              Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
      }
    }

    // Add custom JDBC dialects
    FeaturestoreHelper.registerCustomJdbcDialects();

    // Read the onDemandFeaturegroup using Spark
    return FeaturestoreHelper.getOnDemandFeaturegroup(getSpark(), onDemandFeaturegroupDTO.getQuery(),
        jdbcConnectionString);
  }

  /**
   * Gets a cached featuregroup from a particular featurestore
   *
   * @return a spark dataframe with the featuregroup
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public Dataset<Row> readCachedFeaturegroup() throws HiveNotEnabled {

    if(hudi){
      // returns spark dataframe from a hudi featuregroup
      return  FeaturestoreHelper.getHudiFeaturegroup(getSpark(), featurestore, hudiArgs, hudiTableBasePath);
    } else {
      // returns spark dataframe from a featuregroup
      return FeaturestoreHelper.getCachedFeaturegroup(getSpark(), name, featurestore, version);
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

  public FeaturestoreReadFeaturegroup setJdbcArguments(Map<String, String> jdbcArguments) {
    this.jdbcArguments = jdbcArguments;
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
