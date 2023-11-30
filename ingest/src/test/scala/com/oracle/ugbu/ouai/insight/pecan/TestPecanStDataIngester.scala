package com.oracle.ugbu.ouai.insight.pecan

import org.scalatest.FunSuite

class TestPecanStDataIngester extends FunSuite {

  def getConfPath = {
    getClass.getResource("/os.params.conf").getPath
  }

  test("Pecan St Ingester") {
    //TODO: Add os.params.conf before running this test on local
    val confPath = f"""--configPath ${getConfPath}"""
    val primaryKeys = """--primaryKeys "dataid,local_15min"""
    val offsetColumn = """--offsetColumn local_15min"""
    val startingOffset = """--startingOffset "2021-07-01""""
    val table = """--table "commercial_unlimited.eg_realpower_15min""""
    val maxOffsetsPerTrigger = """--maxOffsetsPerTrigger "10000""""
   // change bucket folders accordingly.
    val checkpointPath = """--checkPointPath "oci://cds_gbua_cndevcorp_ugbu_ouai_a00m3731u2secdv6cfg2_1@oraclegbudevcorp:/pecan_st_disagg/local/1/_checkpoint""""
    val outputPath = """--outputPath "oci://cds_gbua_cndevcorp_ugbu_ouai_a00m3731u2secdv6cfg2_1@oraclegbudevcorp:/pecan_st_disagg/local/1/raw""""
    val secretsPath = """--secretsPath "oci://cds_gbua_cndevcorp_ugbu_ouai_a00m3731u2secdv6cfg2_1@oraclegbudevcorp:/pecan_st_disagg/db_credentials/"""
    val args = Seq(confPath, primaryKeys, offsetColumn, startingOffset, table, maxOffsetsPerTrigger,
      checkpointPath, outputPath, secretsPath).mkString(" ").split(" ")
    PecanStDataIngester.main(args)
  }

}
