package disque

import (
	"testing"
	"time"
)

const queueName string = "test:Queue"

var serverString string

func init() {
	serverString = "127.0.0.1:7711"
}
func setServerString(server string) {
	serverString = server
}

func AddJob(t *testing.T) {
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	jobId, err := client.AddJob(queueName, []byte("testData"),
		AddJobOptions{Replicate: 0,
			Retry:   10 * time.Second,
			MaxLen:  100,
			Async:   false,
			Timeout: 10 * time.Millisecond,
		})
	if err != nil {
		t.Error(err)
		return

	}
	t.Log("Added Job with Job Id", jobId)
	//client.AckJob(jobId)

}
func TestClient_AddJob(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	result, err := client.GetJobWithCounters(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}
	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body), job.Nack, job.AdditionalDeliveries)
		resp, err := client.AckJob(job.Id)
		client.AckJob(job.Id)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Accknoledge Return Value", resp)

	}

}

func TestClient_GetJobWithCounters(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	result, err := client.GetJobWithCounters(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}
	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body), job.Nack, job.AdditionalDeliveries)
		resp, err := client.AckJob(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Accknoledge Return Value", resp)

	}

}
func TestClient_GetJob(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	result, err := client.GetJob(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}
	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body))
		resp, err := client.AckJob(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Accknoledge Return Value", resp)

	}

}

func TestClient_FastAck(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	result, err := client.GetJob(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}
	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body))
		resp, err := client.FastAck(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Accknoledge Return Value", resp)
		if resp == 0 {
			t.Error("Unable to ackknowledge")
		}

	}
}
func TestClient_Working(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	result, err := client.GetJob(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}

	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body))
		wr, err := client.Working(job.Id)
		if err != nil {
			t.Error(err.Error())
			return
		}
		if wr == 0 {
			t.Error("Working Failed", wr)
		}

		resp, err := client.AckJob(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Accknoledge Return Value", resp)
		if resp == 0 {
			t.Error("Unable to ackknowledge")
		}

	}

}
func TestClient_Nack(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	result, err := client.GetJob(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}
	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body))
		resp, err := client.Nack(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
		t.Log("Negative Accknoledge Return Value", resp)
		if resp == 0 {
			t.Error("Unable to Negative acknowledge")
		}
		client.AckJob(job.Id)

	}
}

func TestClient_QLen(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	qlen, err := client.QLen(queueName)
	result, err := client.GetJob(GetJobOptions{Timeout: 1 * time.Millisecond}, queueName)
	if err != nil {
		t.Error(err)
		return
	}
	if qlen == 0 {
		t.Error("Queue Length is 0 ")
	} else {
		t.Log("Qlen", qlen)
	}
	for _, job := range result {
		t.Log(job.QueueName, job.Id, string(job.Body))
		client.AckJob(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
	}

}
func TestClient_QPeek(t *testing.T) {
	AddJob(t)
	client := NewDisqueClient(DisqueClientOptions{}, serverString)
	jobs, err := client.QPeek(queueName, 10)
	if err != nil {
		t.Error(err)
		return
	}

	for _, job := range jobs {
		t.Log(job.QueueName, job.Id, string(job.Body))
		resp, err := client.AckJob(job.Id)

		if err != nil {
			t.Error(err)
			return
		}
		t.Log("acknowledge", resp)
	}

}
