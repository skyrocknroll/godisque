package disque

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

type client struct {
	pools []*redis.Pool
}

//DisqueClientOptions has all the configuration for new connection
type DisqueClientOptions struct {
	// Password for Disque
	Password string
	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool
}
type job struct {
	QueueName string
	Id        string
	Body      []byte
}
type jobWithCounters struct {
	QueueName            string
	Id                   string
	Body                 []byte
	Nack                 int64
	AdditionalDeliveries int64
}

type AddJobOptions struct {
	Timeout   time.Duration
	Replicate int
	Delay     time.Duration
	Retry     time.Duration
	TTL       time.Duration
	MaxLen    int
	Async     bool
}

type GetJobOptions struct {
	NoHang  bool
	Timeout time.Duration
	Count   int
}

type QScanOptions struct {
	Count      int
	BusyLoop   bool
	MinLen     int
	MaxLen     int
	ImportRate int
}

func NewDisqueClient(options DisqueClientOptions, servers ...string) *client {
	c := &client{
		pools: make([]*redis.Pool, len(servers)),
	}

	for i, server := range servers {
		c.pools[i] = &redis.Pool{
			MaxIdle:     options.MaxIdle,
			IdleTimeout: options.IdleTimeout * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				if options.Password != "" {
					if _, err := c.Do("AUTH", options.Password); err != nil {
						c.Close()
						return nil, err
					}
					return c, err

				}
				return c, err

			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < time.Minute {
					return nil
				}
				_, err := c.Do("PING")
				return err
			},
			Wait:      options.Wait,
			MaxActive: options.MaxActive,
		}
	}
	return c
}
func (c *client) get() (redis.Conn, error) {
	var err error
	for _, pool := range c.pools {
		conn := pool.Get()
		if conn.Err() == nil {
			return conn, nil
		} else {
			err = conn.Err()

		}

	}
	return nil, err
}

func (c *client) Close() {

	for _, pool := range c.pools {
		pool.Close()
	}
}

func (c *client) AddJob(queueName string, job []byte, options AddJobOptions) (string, error) {
	conn, err := c.get()

	if err != nil {
		return "", err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{queueName, job, int(options.Timeout.Nanoseconds() / 1000000)}
	if options.Replicate > 0 {
		args = args.Add("REPLICATE", options.Replicate)
	}
	if options.Delay > 0 {
		args = args.Add("DELAY", options.Delay.Seconds())
	}
	if options.Retry >= 0 {
		args = args.Add("RETRY", options.Retry.Seconds())
	}
	if options.TTL > 0 {
		args = args.Add("TTL", options.TTL.Seconds())
	}
	if options.MaxLen > 0 {
		args = args.Add("MAXLEN", options.MaxLen)
	}
	if options.Async == true {
		args = args.Add("ASYNC")
	}
	return redis.String(conn.Do("ADDJOB", args...))
}

//GetJob returns a job
func (c *client) GetJob(options GetJobOptions, queueNames ...string) ([]job, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}

	if options.NoHang == true {
		args = args.Add("NOHANG")
	}
	if options.Timeout.Nanoseconds() > 0 {
		args = args.Add("TIMEOUT", int(options.Timeout.Nanoseconds()/1000000))
	}
	if options.Count > 0 {
		args = args.Add("COUNT", options.Count)
	}
	args = args.Add("FROM")
	for _, queueName := range queueNames {
		args = args.Add(queueName)
	}
	reply, err := redis.Values(conn.Do("GETJOB", args...))
	if err != nil {
		return nil, err
	}

	result := make([]job, 0, len(reply))
	for _, v := range reply {
		if value, err := redis.Values(v, nil); err != nil {
			return nil, err
		} else {
			queueName, err := redis.String(value[0], nil)
			id, err := redis.String(value[1], err)
			body, err := redis.Bytes(value[2], err)
			if err != nil {
				return nil, err
			}
			result = append(result, job{QueueName: queueName, Id: id, Body: body})
		}
	}
	return result, nil
}

//GetJobWithCounters returns a job details with counters details
func (c *client) GetJobWithCounters(options GetJobOptions, queueNames ...string) ([]jobWithCounters, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}

	if options.NoHang == true {
		args = args.Add("NOHANG")
	}
	if options.Timeout.Nanoseconds() > 0 {
		args = args.Add("TIMEOUT", int(options.Timeout.Nanoseconds()/1000000))
	}
	if options.Count > 0 {
		args = args.Add("COUNT", options.Count)
	}
	args = args.Add("WITHCOUNTERS")
	args = args.Add("FROM")
	for _, queueName := range queueNames {
		args = args.Add(queueName)
	}
	reply, err := redis.Values(conn.Do("GETJOB", args...))
	if err != nil {
		return nil, err
	}

	result := make([]jobWithCounters, 0, len(reply))
	for _, v := range reply {
		if value, err := redis.Values(v, nil); err != nil {
			return nil, err
		} else {
			queueName, err := redis.String(value[0], nil)
			id, err := redis.String(value[1], err)
			body, err := redis.Bytes(value[2], err)
			var nack, additionalDeliveries int64
			nack, err = redis.Int64(value[4], err)
			additionalDeliveries, err = redis.Int64(value[6], err)
			if err != nil {
				return nil, err
			}
			result = append(result, jobWithCounters{QueueName: queueName, Id: id, Body: body, Nack: nack, AdditionalDeliveries: additionalDeliveries})
		}
	}
	return result, nil
}

func (client *client) AckJob(jobIds ...string) (int, error) {
	conn, err := client.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("ACKJOB", args...))

}

func (client *client) FastAck(jobIds ...string) (int, error) {
	conn, err := client.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("FASTACK", args...))

}
func (client *client) Working(jobId string) (int, error) {
	conn, err := client.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	args = args.Add(jobId)

	if err != nil {
		return 0, err
	}
	return redis.Int(conn.Do("WORKING", args...))

}

func (client *client) Nack(jobIds ...string) (int, error) {
	conn, err := client.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}

	return redis.Int(conn.Do("NACK", args...))

}

func (c *client) QLen(queueName string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	return redis.Int(conn.Do("QLEN", queueName))
}
func (c *client) QPeek(queueName string, count int) ([]job, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	reply, err := redis.Values(conn.Do("QPEEK", queueName, count))
	if err != nil {
		return nil, err
	}

	result := make([]job, 0, len(reply))
	for _, v := range reply {
		if value, err := redis.Values(v, nil); err != nil {
			return nil, err
		} else {
			queueName, err := redis.String(value[0], nil)
			id, err := redis.String(value[1], err)
			data, err := redis.Bytes(value[2], err)
			if err != nil {
				return nil, err
			}
			result = append(result, job{QueueName: queueName, Id: id, Body: data})
		}
	}
	return result, nil
}

func (c *client) Enqueue(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("ENQUEUE", args...))
}

func (c *client) Dequeue(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("DEQUEUE", args...))
}

func (c *client) DelJob(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("DELJOB", args...))
}

func (c *client) Show(jobId string) (map[string]interface{}, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	reply, err := redis.Values(conn.Do("SHOW", jobId))
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i := 0; i < len(reply); i += 2 {
		if key, ok := reply[i].(string); ok {
			result[key] = reply[i+1]
		} else {
			return nil, errors.New(fmt.Sprintf("interface can not case string: %v", reply[i]))
		}
	}

	return result, nil
}

func (c *client) QScan(options QScanOptions) ([]interface{}, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	} else {
		if conn != nil {
			defer conn.Close()
		}

	}

	args := redis.Args{}
	if options.Count > 0 {
		args = args.Add("COUNT", options.Count)
	}
	if options.BusyLoop == true {
		args = args.Add("BUSYLOOP")
	}
	if options.MinLen > 0 {
		args = args.Add("MINLEN", options.MinLen)
	}
	if options.MaxLen > 0 {
		args = args.Add("MAXLEN", options.MaxLen)
	}
	if options.ImportRate > 0 {
		args = args.Add("IMPORTRATE", options.ImportRate)
	}

	return redis.Values(conn.Do("QSCAN", args...))
}
