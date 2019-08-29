package com.test.kafka.createData;


import com.test.Utils.PropertyUtil;
import com.test.flinktomysqlAndhbase.DataToKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataToKafkaThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DataToKafka.class);
    private static KafkaProducer producer;
    private static Properties props;
    private static String vinArray[] = {"LLNCHSG62728", "LLNCHSG6272878761", "LLNCHSG6272878762", "LLNCHSG6272878763", "LLNCHSG6272878764", "LLNCHSG6272878765", "LLNCHSG6272878766", "LLNCHSG6272878767", "LLNCHSG6272878768"};
    private static String busNameArr[] = {"PTCAN3,2A3,BMS_AlmHV", "PTCAN3,F4,BMS_AlmLv", "PTCAN3,F4,BMS_Hvil1", "PTCAN3,F4,BMS_Hvil2", "PTCAN3,2A0,BMS_InletTemp", "PTCAN3,3A3,BMS_Insulation_R", "PTCAN3,3A2,BMS_MaxCellU", "PTCAN3,3A3,BMS_MaxTemp", "PTCAN3,3A2,BMS_MinCellU", "PTCAN3,3A3,BMS_MinTemp", "PTCAN3,2A0,BMS_OutletTemp", "PTCAN3,2A1,BMS_PackSocAct", "PTCAN3,F5,BMS_PackUinside", "PTCAN3,F4,BMS_State", "PTCAN3,285,CoolFanPwm", "PTCAN3,2AB,DCDC_ErrorStatus", "PTCAN3,285,EDSPumpSts", "PTCAN3,1AE,EPS_EPBStatus", "PTCAN3,B1,ESP_VehicleSpeed", "PTCAN3,B1,ESP_VehicleSpeed_Valid", "PTCAN3,1FF,GsRTPState", "PTCAN3,285,HeatPumpSts", "PTCAN3,2AB,IPEU_Temp", "PTCAN3,FD,IPEU_UdcHvCurr", "PTCAN3,FD,IPEU_UdcLvCurr", "PTCAN3,E4,MCUF_ErrorStatus", "PTCAN3,E4,MCUF_HVIL", "PTCAN3,E4,MCUF_State", "PTCAN3,E4,MCUF_TempCoolant", "PTCAN3,E5,MCUR_ErrorStatus", "PTCAN3,E5,MCUR_HVIL", "PTCAN3,E5,MCUR_State", "PTCAN3,E5,MCUR_TempCoolant", "PTCAN3,FC,OBC_DCVoltage", "PTCAN3,2AB,OBC_ErrorStatus", "PTCAN3,2AA,OBC_SocketTemp1", "PTCAN3,2AA,OBC_SocketTemp2", "PTCAN3,FA,PDU_ErrorStatus", "PTCAN3,285,RESSPumpSts", "PTCAN3,282,VDCM_4ValveSts", "PTCAN3,284,VDCM_Chiller3ValveSts", "PTCAN3,284,VDCM_ECFSts", "PTCAN3,D4,VDCM_FaultLevel", "PTCAN3,D4,VDCM_HVLock_Reserved", "PTCAN3,284,VDCM_LTR3ValveSts", "PTCAN3,284,VDCM_Mot3ValveSts", "PTCAN3,284,VDCM_MotorInletT"};
    private static String valueArr[] = {"BMS_AlmHV,1", "BMS_AlmLv,0", "BMS_Hvil1,0", "BMS_Hvil2,0", "BMS_InletTemp,40", "BMS_Insulation_R,2.5", "BMS_MaxCellU,4.18", "BMS_MaxTemp,55", "BMS_MinCellU,2.8", "BMS_PackSocAct,80", "BMS_PackUinside,468", "BMS_State,8", "DCDC_ErrorStatus,0", "ESP_VehicleSpeed_Valid,0", "GsRTPState,2", "IPEU_Temp,65", "IPEU_UdcLvCurr,11.5", "MCUF_ErrorStatus,0", "MCUF_HVIL,0", "MCUF_TempCoolant,75", "MCUR_HVIL,0", "MCUR_TempCoolant,75", "OBC_ErrorStatus,0", "PDU_ErrorStatus,0", "VDCM_FaultLevel,2"};
    private static Map<String, Double> valueMap;
    static {
        props = new Properties();
        //声明Kakfa相关信息
        props.put("bootstrap.servers", PropertyUtil.getProperty("kafka.bootstrap.servers"));
        props.put("key.serializer", PropertyUtil.getProperty("kafka.key.serializer")); //key 序列化
        props.put("value.serializer", PropertyUtil.getProperty("kafka.value.serializer")); //value 序列化
        props.put("request.required.acks", PropertyUtil.getProperty("kafka.request.required.acks"));
        producer = new KafkaProducer<String, String>(props);
    }
    private String vin;
    public DataToKafkaThread(String vin) {
        this.vin = vin;
    }
    @Override
    public void run() {
        while (true) {
            for (String str : busNameArr) {
                StringBuffer sb = new StringBuffer();
                sb.append(str);
                sb.append(",");
                if (valueMap.containsKey(str.split(",")[2])) {
                    sb.append(Math.abs(valueMap.get(str.split(",")[2]) - (Math.random() * 10)));
                } else {
                    sb.append(Math.abs(Math.random() * 10));
                }
                sb.append(",");
                sb.append(vin);
                sb.append(",");
                sb.append(new Date().getTime());
                producer.send(new ProducerRecord<String, String>(PropertyUtil.getProperty("kafka.topic.name"), sb.toString()));
                System.out.println(sb.toString());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info(e.getMessage());
                }
            }
        }
    }
    public static void main(String[] args) throws InterruptedException {
        valueMap = new HashMap<String, Double>();
        for (String v : valueArr) {
            String[] vv = v.split(",");
            if (vv.length == 2) {
                try {
                    valueMap.put(vv[0], Double.valueOf(vv[1]));
                } catch (Exception e) {
                    logger.info(e.getMessage());
                }
            }
        }
        int threadCount = 10;
        for (String vin : vinArray) {
            for (int i = 0; i < threadCount; i++) {
                Thread t = new Thread(new DataToKafkaThread(vin), "线程" + i);
                t.start();
                logger.info("Thread name {} id {} start", t.getName(), t.getId());
            }
        }
    }

}