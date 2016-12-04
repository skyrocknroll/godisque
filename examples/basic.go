package main

import (
	"github.com/skyrocknroll/godisque/disque"
	"log"
	"time"
)

func main() {
	conn := disque.NewDisqueClient(disque.DisqueClientOptions{}, "127.0.0.1:7711")

	// ADDJOB
	replicate := 1
	delay := time.Second * 1
	retry := time.Second * 300
	ttl := time.Second * 86400
	queueName := "TestQueue"
	for i := 0; i < 2; i++ {
		jobId, err := conn.AddJob(
			queueName,
			[]byte("JobData"),
			disque.AddJobOptions{
				Replicate: replicate,
				Delay:     delay,
				Retry:     retry,
				TTL:       ttl,
				MaxLen:    10,
				Async:     true,
			},
		)
		if err != nil {
			log.Println(err.Error())
		}
		log.Printf("Added job with Id %s", jobId)

	}

	// GETJOB
	jobs, err := conn.GetJob(disque.GetJobOptions{
		Count:   1,
		NoHang:  false,
		Timeout: time.Second * 60,
	},
		queueName)
	if err != nil {
		log.Println("Get error", err.Error())
		return
	}
	for _, job := range jobs {
		log.Println(job.QueueName, job.Id, string(job.Body))
		log.Println(conn.AckJob(job.Id))
	}

	// GETJOB WITHCOUNTERS
	jobsWithCounters, err := conn.GetJobWithCounters(disque.GetJobOptions{
		Count:   1,
		NoHang:  false,
		Timeout: time.Second * 60,
	},
		queueName)
	if err != nil {
		log.Println("Get error", err.Error())
		return
	}
	for _, job := range jobsWithCounters {
		log.Println(job.QueueName, job.Id, string(job.Body), job.Nack, job.AdditionalDeliveries)
		log.Println(conn.AckJob(job.Id))
	}

}
