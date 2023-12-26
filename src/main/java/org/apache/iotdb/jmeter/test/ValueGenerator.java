package org.apache.iotdb.jmeter.test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

public class ValueGenerator {
  private final String[] devices = {
      "cent_9_Humidity",
      "side_8_Humidity",
      "side_7_Humidity",
      "ang_30_Humidity",
      "ang_45_Humidity",
      "ang_60_Humidity",
      "ang_90_Humidity",
      "bef_1195_Humidity",
      "aft_1120_Humidity",
      "mid_1125_Humidity",
      "cor_4_Humidity",
      "cor_1_Humidity",
      "cor_5_Humidity",
      "cent_9_Power",
      "side_8_Power",
      "side_7_Power",
      "ang_30_Power",
      "ang_45_Power",
      "ang_60_Power",
      "ang_90_Power",
      "bef_1195_Power",
      "aft_1120_Power",
      "mid_1125_Power",
      "cor_4_Power",
      "cor_1_Power",
      "cor_5_Power",
      "cent_9_Pressure",
      "side_8_Pressure",
      "side_7_Pressure",
      "ang_30_Pressure",
      "ang_45_Pressure",
      "ang_60_Pressure",
      "ang_90_Pressure",
      "bef_1195_Pressure",
      "aft_1120_Pressure",
      "mid_1125_Pressure",
      "cor_4_Pressure",
      "cor_1_Pressure",
      "cor_5_Pressure",
      "cent_9_Flow",
      "side_8_Flow",
      "side_7_Flow",
      "ang_30_Flow",
      "ang_45_Flow",
      "ang_60_Flow",
      "ang_90_Flow",
      "bef_1195_Flow",
      "aft_1120_Flow",
      "mid_1125_Flow",
      "cor_4_Flow",
      "cor_1_Flow",
      "cor_5_Flow",
      "cent_9_Level",
      "side_8_Level",
      "side_7_Level",
      "ang_30_Level",
      "ang_45_Level",
      "ang_60_Level",
      "ang_90_Level",
      "bef_1195_Level",
      "aft_1120_Level",
      "mid_1125_Level",
      "cor_4_Level",
      "cor_1_Level",
      "cor_5_Level",
      "cent_9_Temperature",
      "side_8_Temperature",
      "side_7_Temperature",
      "ang_30_Temperature",
      "ang_45_Temperature",
      "ang_60_Temperature",
      "ang_90_Temperature",
      "bef_1195_Temperature",
      "aft_1120_Temperature",
      "mid_1125_Temperature",
      "cor_4_Temperature",
      "cor_1_Temperature",
      "cor_5_Temperature",
      "cent_9_vibration",
      "side_8_vibration",
      "side_7_vibration",
      "ang_30_vibration",
      "ang_45_vibration",
      "ang_60_vibration",
      "ang_90_vibration",
      "bef_1195_vibration",
      "aft_1120_vibration",
      "mid_1125_vibration",
      "cor_4_vibration",
      "cor_1_vibration",
      "cor_5_vibration",
      "cent_9_tilt",
      "side_8_tilt",
      "side_7_tilt",
      "ang_30_tilt",
      "ang_45_tilt",
      "ang_60_tilt",
      "ang_90_tilt",
      "bef_1195_tilt",
      "aft_1120_tilt",
      "mid_1125_tilt",
      "cor_4_tilt",
      "cor_1_tilt",
      "cor_5_tilt",
      "cent_9_level",
      "side_8_level",
      "side_7_level",
      "ang_30_level",
      "ang_45_level",
      "ang_60_level",
      "ang_90_level",
      "bef_1195_level",
      "aft_1120_level",
      "mid_1125_level",
      "cor_4_level",
      "cor_1_level",
      "cor_5_level",
      "cent_9_level_vibrating",
      "side_8_level_vibrating",
      "side_7_level_vibrating",
      "ang_30_level_vibrating",
      "ang_45_level_vibrating",
      "ang_60_level_vibrating",
      "ang_90_level_vibrating",
      "bef_1195_level_vibrating",
      "aft_1120_level_vibrating",
      "mid_1125_level_vibrating",
      "cor_4_level_vibrating",
      "cor_1_level_vibrating",
      "cor_5_level_vibrating",
      "cent_9_level_rotating",
      "side_8_level_rotating",
      "side_7_level_rotating",
      "ang_30_level_rotating",
      "ang_45_level_rotating",
      "ang_60_level_rotating",
      "ang_90_level_rotating",
      "bef_1195_level_rotating",
      "aft_1120_level_rotating",
      "mid_1125_level_rotating",
      "cor_4_level_rotating",
      "cor_1_level_rotating",
      "cor_5_level_rotating",
      "cent_9_level_admittance",
      "side_8_level_admittance",
      "side_7_level_admittance",
      "ang_30_level_admittance",
      "ang_45_level_admittance",
      "ang_60_level_admittance",
      "ang_90_level_admittance",
      "bef_1195_level_admittance",
      "aft_1120_level_admittance",
      "mid_1125_level_admittance",
      "cor_4_level_admittance",
      "cor_1_level_admittance",
      "cor_5_level_admittance",
      "cent_9_Pneumatic_level",
      "side_8_Pneumatic_level",
      "side_7_Pneumatic_level",
      "ang_30_Pneumatic_level"
  };

  public String get_device() {
    Random random = new Random();
    int randomNumber = random.nextInt(160);
    String device = devices[randomNumber];
    return device;
  }

  public String get_value(String key) {
    int size = 1000;

    String iotParameter = null;
    if (key.contains(":")) {
      iotParameter = key.split(":")[1];
    } else
      iotParameter = key;
    Random r = new Random();
    StringBuilder sb = new StringBuilder(size);
    sb.append(iotParameter);
    BigDecimal val = BigDecimal.valueOf(r.nextDouble()).setScale(4, RoundingMode.HALF_UP);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(iotParameter);
      sb.append('_');
      sb.append("value");
      sb.append(':');
      sb.append(val);
      sb.append(':');
      sb.append("timestamp");
      sb.append(':');
      sb.append(System.currentTimeMillis());
      sb.append(":");
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);
    return sb.toString();
  }
}
