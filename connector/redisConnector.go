package connector

import (
	"errors"
	"sync"
	"time"

	redis "gopkg.in/redis.v5"
)

var (
	ErrNoSuchEntity = errors.New("redis: no such entity")
)

type Unmarshal func(bytes []byte) error

type redisConnector struct {
	conn *redis.Client
}

type RedisCacher interface {
	Ping() error
	Close() error
	GetObject(key string, f Unmarshal) error
	RemoveObject(keys string) error
	CreateObject(key string, bytes []byte, expirationTime time.Duration) error
	SetExpiration(key string, expirationTime time.Duration)
}

var onceRedis sync.Once
var rConnector redisConnector

func NewRedisSingleInstanceConnector(connectionChain string) RedisCacher {
	onceRedis.Do(func() {
		rConnector.conn = redis.NewClient(&redis.Options{
			Addr: connectionChain,
		})

	})

	return &rConnector
}

func NewRedisConnector(connectionChain string) RedisCacher {
	rConnector := new(redisConnector)
	rConnector.conn = redis.NewClient(&redis.Options{
		Addr: connectionChain,
	})
	return rConnector
}

func (r *redisConnector) Close() (err error) {
	return r.conn.Close()
}

func (r *redisConnector) Ping() (err error) {
	_, err = r.conn.Ping().Result()
	return
}

func (r *redisConnector) GetObject(key string, f Unmarshal) (err error) {
	bytes, e := r.conn.Get(key).Bytes()
	if e != nil {
		return ErrNoSuchEntity
	}

	if err = f(bytes); err != nil {
		return errors.New("redis: error while unmarshal: " + err.Error())
	}

	return err
}

func (r *redisConnector) RemoveObject(key string) error {
	return r.conn.Del(key).Err()
}

func (r *redisConnector) CreateObject(key string, bytes []byte, expirationTime time.Duration) error {
	return r.conn.Set(key, bytes, expirationTime).Err()
}

func (r *redisConnector) SetExpiration(key string, expirationTimeInSec time.Duration) {
	r.conn.ExpireAt(key, time.Now().Local().Add(expirationTimeInSec))
}
