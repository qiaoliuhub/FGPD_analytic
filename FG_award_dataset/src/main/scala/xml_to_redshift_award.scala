import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, concat, lit, udf}
import com.typesafe.config._
import scala.collection.JavaConversions._
import org.apache.log4j._



object xml_to_redshift_award {
    
    
    //Flatten a tree structure of XML schema 
    def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
        schema.fields.flatMap(f => {
        val colName = if (prefix == null) f.name else (prefix + "." + f.name)

        f.dataType match {
            case st: StructType => flattenSchema(st, colName)
            case _ => Array(col(colName))
                }
            })
    }
    
    def main(args: Array[String]) {

        val log = org.apache.log4j.LogManager.getLogger("ApplicationLogger")

        try {

            val conf = ConfigFactory.load("application")
        
            //Input ACCESS_KEY, SECRET_KEY and Bucket from config file
            val ACCESS_KEY=conf.getString("my.aws.ACCESS_KEY")
            val SECRET_KEY=conf.getString("my.aws.SECRET_KEY")
            val Bucket=conf.getString("my.aws.S3_bucket")
            val JDBC_URL=conf.getString("my.aws.JDBC_URL")
            val file_name=args(0)
            val year=args(1)
        
            val sc = new SparkContext() 
        
            sc.hadoopConfiguration.set("fs.s3a.access.key", ACCESS_KEY)
            sc.hadoopConfiguration.set("fs.s3a.secret.key", SECRET_KEY)
            val sqlContext = new SQLContext(sc)
            import sqlContext.implicits._
        
            //Construct the name of input file which is on s3, for example, "s3a://de-ny-qiao/FY05-V1.4/*AWARD.xml"
            val downloadpath=Bucket+file_name+"/*AWARD.xml"
            //Construct name of redshift table, to which these data will be saved to
            val contract_table_name="award_contract_"+year
            val vendor_table_name="award_vendor_"+year
        
            //Download files from s3 and parse XML data 
            val df = sqlContext.read.format("com.databricks.spark.xml")
                    .option("excludeAttribute", "True")
                        .option("rowTag", "ns1:award")
                            .load(downloadpath)
                                .withColumn("ns1:unique_award_id", concat($"ns1:awardID.ns1:awardContractID.ns1:PIID", lit("_"), $"ns1:awardID.ns1:awardContractID.ns1:agencyID", lit("_"), $"ns1:awardID.ns1:awardContractID.ns1:modNumber", lit("_"), $"ns1:awardID.ns1:awardContractID.ns1:transactionNumber"))
                                    .withColumn("ns1:referencedID", concat($"ns1:awardID.ns1:referencedIDVID.ns1:PIID", lit("_"), $"ns1:awardID.ns1:referencedIDVID.ns1:agencyID", lit("_"), $"ns1:awardID.ns1:referencedIDVID.ns1:modNumber"))
                                        .persist
                    
            //Faltten data frame df's schema
            val whole_schema=flattenSchema(df.schema)

            //Filter out vendor table's information from contract table and only save vendor table's distkey DUNSNumber
            val contract_schema=whole_schema
                        .filter(x=>(!(x.toString.startsWith("ns1:vendor") | x.toString.startsWith("ns1:awardID")))):+ $"ns1:vendor.ns1:vendorSiteDetails.ns1:vendorDUNSInformation.ns1:DUNSNumber"

            val renameList = conf.getConfigList("my.schema.rename") map (paras => (paras.getString("name"), paras.getString("newname")))
            
            val rename=renameList.toList.toMap
        
            //Rename column if needed
            val contract_table_schema = contract_schema
                        .map(name => name.as(rename.getOrElse(name.toString, name.toString)))
                        .map(name => name.as(name.toString.substring(name.toString.lastIndexOf(":")+1, name.toString.length()-1)))
                        
            var contract_award_table=df.select(contract_table_schema:_*)
         
            //Specify the width of column whose data is text and longer than 256 characters
            val columnLengthMap = conf.getConfigList("my.schema.columnLengthMap") map (paras => (paras.getString("name"), paras.getInt("length"))).toList.toMap 
        
            println(columnLengthMap)

            // Apply each column metadata customization
            columnLengthMap.foreach { case (colName, length) =>
                if (contract_award_table.columns contains colName){
                    val metadata = new MetadataBuilder().putLong("maxlength", length).build()
                    contract_award_table = contract_award_table.withColumn(colName, contract_award_table(colName).as(colName, metadata))
                    }
                }
        
            // Load contract_award_table to Redshift
            contract_award_table
                   .write
                   .format("com.databricks.spark.redshift")
                   .option("url", JDBC_URL)
                   .option("dbtable", contract_table_name)
                   .option("tempdir", Bucket+"temp_file_award_contract")
                   .option("forward_spark_s3_credentials","True")
                   .option("tempformat","CSV GZIP")
                   .option("diststyle","KEY")
                   .option("distkey", "dunsnumber")
                   .option("sortkeyspec","SORTKEY(signeddate)")
                   .option("extracopyoptions","TRUNCATECOLUMNS")
                   .option("extracopyoptions","MAXERROR 50")
                   .mode("error")
                   .save()
        
            //Construct the vendor table with DUNSNumber as uniqueid
            val vendor_schema=whole_schema.filter(_.toString.startsWith("ns1:vendor"))
                
            val vendor_table_schema=vendor_schema.map(name => name.as(name.toString.substring(name.toString.lastIndexOf(":")+1)))
        
            val vendor_award_table=df.select(vendor_table_schema:_*).dropDuplicates("dunsnumber")
            
            vendor_award_table.printSchema
        
            vendor_award_table.write
                    .format("com.databricks.spark.redshift")
                    .option("url", JDBC_URL)
                    .option("dbtable", vendor_table_name)
                    .option("tempdir", Bucket+"temp_file_award_contract")
                    .option("forward_spark_s3_credentials","True")
                    .option("tempformat","CSV GZIP")
                    .option("diststyle","KEY")
                    .option("distkey", "dunsnumber")
                    .option("sortkeyspec","SORTKEY(dunsnumber)")
                    .option("extracopyoptions","TRUNCATECOLUMNS")
                    .option("extracopyoptions","MAXERROR 50")
                    .mode("error")
                    .save()
            }
            catch{
                case ae: AnalysisException => {log.error("Can't resolve column name: " + ae.toString, ae)}
                case ce: ConfigException => {log.error("Values in configuration file are missing or in wrong types: " + ce.origin(), ce)}
                case se: SparkException => {log.error(se.toString(), se)}
                case fe: FileNotFoundException => {log.error ("Missing files:"+ fe.toString(), fe)}
                case urle: MalformedURLException => {log.error ("Got a bad URL:"+ urle.toString(), urle)}
                case e: Exception => {log.error ("Got an Exception:"+ e.toString(), e)}
            }
            finally{
                sc.stop()
            }
            
    }
     
}