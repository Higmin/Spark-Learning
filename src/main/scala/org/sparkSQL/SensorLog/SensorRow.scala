package org.sparkSQL.SensorLog

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

case class SensorRow(
                      androidBoardAvailableStorage: String, androidBoardCpu_usage: String, androidBoardCurrent: String,
                      androidBoardStorage: String, androidBoardVoltage: String, axialFanSpeed: String,
                      backLightState: String, blEnableState: String, crossFlowFanSpeedValue: String,
                      decibelValue: String, doorStateState: String, spsLatitude: String,
                      gpsLongitude: String, humidityValue: String, levelValue: String,
                      powerStateState: String, powerStateValue: String, temperature: String,
                      totalPowerValue: String, time: Long)

object SensorRow extends Serializable {

  /**
    * 列族 “info”
    * @param result
    * @return
    */
  def parseSensorRow(result: Result): SensorRow = {
    val p0 = Bytes.toString(result.getValue("info".getBytes, "AndroidBoard::availableStorage".getBytes))
    val p1 = Bytes.toString(result.getValue("info".getBytes, "AndroidBoard::cpu_usage".getBytes))
    val p2 = Bytes.toString(result.getValue("info".getBytes, "AndroidBoard::current".getBytes))
    val p3 = Bytes.toString(result.getValue("info".getBytes, "AndroidBoard::storage".getBytes))
    val p4 = Bytes.toString(result.getValue("info".getBytes, "AndroidBoard::voltage".getBytes))
    val p5 = Bytes.toString(result.getValue("info".getBytes, "AxialFanSpeed".getBytes))
    val p6 = Bytes.toString(result.getValue("info".getBytes, "BackLight::state".getBytes))
    val p7 = Bytes.toString(result.getValue("info".getBytes, "BlEnable::state".getBytes))
    val p8 = Bytes.toString(result.getValue("info".getBytes, "CrossFlowFanSpeed::value".getBytes))
    val p9 = Bytes.toString(result.getValue("info".getBytes, "Decibel::value".getBytes))
    val p10 = Bytes.toString(result.getValue("info".getBytes, "DoorState::state".getBytes))
    val p11 = Bytes.toString(result.getValue("info".getBytes, "GPS::latitude".getBytes))
    val p12 = Bytes.toString(result.getValue("info".getBytes, "GPS::longitude".getBytes))
    val p13 = Bytes.toString(result.getValue("info".getBytes, "Humidity::value".getBytes))
    val p14 = Bytes.toString(result.getValue("info".getBytes, "Level::value".getBytes))
    val p15 = Bytes.toString(result.getValue("info".getBytes, "PowerState::state".getBytes))
    val p16 = Bytes.toString(result.getValue("info".getBytes, "PowerState::value".getBytes))
    val p17 = Bytes.toString(result.getValue("info".getBytes, "Temperature".getBytes))
    val p18 = Bytes.toString(result.getValue("info".getBytes, "TotalPower::value".getBytes))
    val p19 = Bytes.toString(result.getValue("info".getBytes, "Time".getBytes))
    SensorRow(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19.toLong)
  }
}
