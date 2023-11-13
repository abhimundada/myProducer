package main

import (
	"context"
	"encoding/json"
	"github.com/databahn-ai/common-utils/kafka"
	"time"
)

var msg1 = "{\"baseeventid\":\"2370268694\",\"categoryseverity\":\"0\",\"customstring41\":\"0-PPE-1\",\"db_device_type\":\"netscaler_vpn\",\"db_device_vendor\":\"netscaler_vpn\",\"db_event_source_id\":\"aedc7e81-0f9f-4a86-9c01-29e340f9dc82\",\"db_log_type\":\"syslog\",\"db_parser_timestamp\":1698862347156,\"deviceaction\":\"Message\",\"devicehostname\":\"CARDC-SDX01-VPX04\",\"eventtime\":1681061412000,\"message\":\"\\\"Citrix ADC Custom Message Action\",\"rawevent\":\"Apr  9 17:30:12 10.254.19.86 04/09/2022:17:35:14  CARDC-SDX01-VPX04 0-PPE-1 : default REWRITE Message 2370268694 0 :  \\\"Citrix ADC Custom Message Action, Client: 10.245.147.189, Requested Host: wbs.childrens.com, Request Method: GET, Requested URL: /WBS_VAL/ACWebBlobService.ashx?env=VAL&action=18&module=P2P.FAILOVER&token=m7LQZM2g70BWQ3h4TDQcu%2FFB3mEXlko%2BAYA2Uud5dhl8DJsjzlAAIUukKgFX4WAnl1omoimCZZxP7EjKurEmkZ64kkv2sM7CeNuLVhS40x%2BnnUGK%2BYq0ITbAAPfWnlXe&user=REGBATCH, Server: 10.245.154.228, HTTP Response: 400 Bad Request, Client Type: \\\"\",\"sourcehostname\":\"10.254.19.86\",\"sourceservicename\":\"REWRITE\"}"
var msg = "{\"baseeventid\":\"2370268694\",\"categoryseverity\":\"0\",\"customstring41\":\"0-PPE-1\",\"db_device_type\":\"netscaler_vpn\",\"db_device_vendor\":\"netscaler_vpn\",\"db_event_source_id\":\"aedc7e81-0f9f-4a86-9c01-29e340f9dc82\",\"db_log_type\":\"syslog\",\"db_parser_timestamp\":1698862347156,\"deviceaction\":\"Message\",\"devicehostname\":\"CARDC-SDX01-VPX04\",\"eventtime\":1681061412000,\"message\":\"\\\"Citrix ADC Custom Message Action\",\"rawevent\":\"Apr  9 17:30:12 10.254.19.86 04/09/2022:17:35:14  CARDC-SDX01-VPX04 0-PPE-1 : default REWRITE Message 2370268694 0 :  \\\"Citrix ADC Custom Message Action, Client: 10.245.147.189, Requested Host: wbs.childrens.com, Request Method: GET, Requested URL: /WBS_VAL/ACWebBlobService.ashx?env=VAL&action=18&module=P2P.FAILOVER&token=m7LQZM2g70BWQ3h4TDQcu%2FFB3mEXlko%2BAYA2Uud5dhl8DJsjzlAAIUukKgFX4WAnl1omoimCZZxP7EjKurEmkZ64kkv2sM7CeNuLVhS40x%2BnnUGK%2BYq0ITbAAPfWnlXe&user=REGBATCH, Server: 10.245.154.228, HTTP Response: 400 Bad Request, Client Type: \\\"\",\"sourcehostname\":\"10.254.19.86\",\"sourceservicename\":\"REWRITE\"}"

func main() {
	cluster := kafka.NewKafkaCluster("abhishek", "localhost:9092")

	c := kafka.ProducerConfig{
		Name:       "abhishek",
		Topic:      "db.destination.syslog",
		ExtraParam: nil,
	}
	p, _ := cluster.NewProducer(context.Background(), c)

	for i := 0; i < 3000; i++ {
		p.SendSync(context.Background(), getMsg())
		//p.SendSync(context.Background(), getMsg1())
		time.Sleep(10 * time.Millisecond)
	}
}

func getMsg() kafka.Message {
	bytes, _ := json.Marshal(msg)

	return kafka.Message{
		Key:     nil,
		Message: bytes,
		Headers: getHeaders(),
	}
}

func getMsg1() kafka.Message {
	bytes, _ := json.Marshal(msg1)

	return kafka.Message{
		Key:     nil,
		Message: bytes,
		Headers: getHeaders1(),
	}
}

func getHeaders1() []kafka.Header {
	return []kafka.Header{
		{
			Key:   "destination_id",
			Value: []byte("5bdb2b52-ab87-4f87-bfb4-e9dae6f35cf2"),
		},
		{
			Key:   "db_event_source_id",
			Value: []byte("5bdb2b52-ab87-4f87-bfb4-e9dae6f35cf2"),
		},
	}
}

func getHeaders() []kafka.Header {
	return []kafka.Header{
		{
			Key:   "destination_id",
			Value: []byte("2894405d-0460-4db0-b531-699b1cb37db3"),
		},
		{
			Key:   "db_event_source_id",
			Value: []byte("2894405d-0460-4db0-b531-699b1cb37db3"),
		},
	}
}
