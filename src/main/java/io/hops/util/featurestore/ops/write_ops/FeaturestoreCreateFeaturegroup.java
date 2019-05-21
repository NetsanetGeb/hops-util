package io.hops.util.featurestore.ops.write_ops;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.DataSourceWriteOptions;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hive.HiveSyncConfig;
import com.uber.hoodie.hive.HiveSyncTool;
import com.uber.hoodie.hive.HoodieHiveClient;
import com.uber.hoodie.hive.MultiPartKeysValueExtractor;
import com.uber.hoodie.hive.util.SchemaUtil;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.FeatureDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parquet.schema.MessageType;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Builder class for Create-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreCreateFeaturegroup extends FeaturestoreOp {
  
  private static final Logger LOG = Logger.getLogger(FeaturestoreCreateFeaturegroup.class.getName());
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to create
   */
  public FeaturestoreCreateFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }

  /**
   * Creates a new feature group in the featurestore
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws IOException IOException
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, InvalidPrimaryKeyForFeaturegroup, FeaturegroupCreationError, FeaturestoreNotFound,
    JWTNotFoundException, IOException {
    if(dataframe == null) {
      throw new IllegalArgumentException("Dataframe to create featuregroup from cannot be null, specify dataframe " +
        "with " +
        ".setDataframe(df)");
    }
    primaryKey = FeaturestoreHelper.primaryKeyGetOrDefault(primaryKey, dataframe);
    FeaturestoreHelper.validatePrimaryKey(dataframe, primaryKey);
    FeaturestoreHelper.validateMetadata(name, dataframe.dtypes(), dependencies, description);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, spark, dataframe,
      featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns,
      numBins, numClusters, corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(dataframe.schema(), primaryKey,
      partitionBy);
    String createTableSql = null;
    HiveSyncTool hiveSyncTool = null;
    if(hudi){
      LOG.info("HUDI");
      String tableName = FeaturestoreHelper.getTableName(name, version);
      LOG.info("Got Table Name");
      FeaturestoreHelper.hoodieTable(dataframe, hudiArgs, hudiTableBasePath, tableName);
      LOG.info("Called HoodieTable");
      //String tableName = hudiTableBasePath.substring(hudiTableBasePath.lastIndexOf("/")+1);
      hiveSyncTool = buildHiveSyncTool(tableName);
      LOG.info("Built SyncTool");
      createTableSql = getHudiTableDDLSql(hiveSyncTool);
      LOG.info("Got SQL: " + createTableSql);
      LOG.info("TableName: " + tableName);
    }
    FeaturestoreRestClient.createFeaturegroupRest(featurestore, name, version, description, jobName, dependencies,
      featuresSchema, statisticsDTO, createTableSql);
    FeaturestoreHelper.insertIntoFeaturegroup(dataframe, spark, name,
            featurestore, version, hudi, hudiArgs, hudiTableBasePath, hiveSyncTool);
    //Update metadata cache since we created a new feature group
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
    
  }
  
  private HiveSyncTool buildHiveSyncTool(String tableName){
    TypedProperties props = new TypedProperties();
    props.put(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY(),
      Boolean.valueOf(DataSourceWriteOptions.DEFAULT_HIVE_ASSUME_DATE_PARTITION_OPT_VAL()));
    props.put(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), featurestore);
    props.put(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), tableName);
    props.put(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hudiArgs.get(DataSourceWriteOptions.HIVE_USER_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hudiArgs.get(DataSourceWriteOptions.HIVE_PASS_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hudiArgs.get(DataSourceWriteOptions.HIVE_URL_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(),
      hudiArgs.get(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            MultiPartKeysValueExtractor.class.getName());


    HiveConf hiveConf = new HiveConf(true);
    hiveConf.addResource(getSpark().sparkContext().hadoopConfiguration());
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, hudiTableBasePath);
    FileSystem fs = FSUtils.getFs(hudiTableBasePath, getSpark().sparkContext().hadoopConfiguration());
    HiveSyncTool hiveSyncTool = new HiveSyncTool(hiveSyncConfig, hiveConf, fs);
    return hiveSyncTool;
  }
  
  private String getHudiTableDDLSql(HiveSyncTool hiveSyncTool) throws IOException {
    //Get the HoodieHiveClient
    HoodieHiveClient hoodieHiveClient = hiveSyncTool.getHoodieHiveClient();

    // Get the parquet schema for this dataset looking at the latest commit
    MessageType schema = hoodieHiveClient.getDataSchema();
    HiveSyncConfig cfg = hiveSyncTool.getSyncCfg();
    String createSQLQuery = SchemaUtil.generateCreateDDL(schema, cfg, HoodieInputFormat.class.getName(),
        MapredParquetOutputFormat.class.getName(), ParquetHiveSerDe.class.getName());
    return createSQLQuery;
  }
  
  public FeaturestoreCreateFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setJobName(String jobName) {
    this.jobName = jobName;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDescription(String description) {
    this.description = description;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setPartitionBy(List<String> partitionBy) {
    this.partitionBy = partitionBy;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setHudi(boolean hudi) {
    this.hudi = hudi;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setHudiArgs(Map<String, String> hudiArgs) {
    this.hudiArgs = hudiArgs;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setHudiTableBasePath(String hudiTableBasePath) {
    this.hudiTableBasePath = hudiTableBasePath;
    return this;
  }
  
}
