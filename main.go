package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type ShippingOrder struct {
	gorm.Model

	OrderID string `json:"order_id" gorm:"index"`
	Vendor  string `json:"vendor"`
	Address string `json:"address"`
}

type PlaceShippingOrderRequest struct {
	OrderID string `json:"order_id"`
	Vendor  string `json:"vendor"`
	Address string `json:"address"`
}

type Shipping struct {
	db *gorm.DB
}

func NewShipping(db *gorm.DB) *Shipping {
	return &Shipping{db: db}
}

func (s *Shipping) Save(ctx context.Context, order ShippingOrder) (ShippingOrder, error) {
	err := s.db.WithContext(ctx).Save(&order).Error
	return order, err
}

func (s *Shipping) ByID(ctx context.Context, id uint) (*ShippingOrder, error) {
	return s.by(ctx, "id", id)
}

func (s *Shipping) ByOrderID(ctx context.Context, id uint) (*ShippingOrder, error) {
	return s.by(ctx, "order_id", id)
}
func (s *Shipping) by(ctx context.Context, key string, val any) (*ShippingOrder, error) {
	var order ShippingOrder
	if tr := s.db.WithContext(ctx).Where(key+"=?", val).First(&order); tr.Error != nil {
		return nil, tr.Error
	}
	return &order, nil
}

type Redis[T any] struct {
	client *redis.Client
}

func NewRedis[T any](rdb *redis.Client) *Redis[T] {
	return &Redis[T]{client: rdb}
}

func (r *Redis[T]) Start(ctx context.Context, idempotencyKey string) (T, bool, error) {
	var t T
	tr := r.client.HSetNX(ctx, "idempotency:"+idempotencyKey, "status", "started")
	if tr.Err() != nil {
		return t, false, tr.Err()
	}
	if tr.Val() {
		return t, false, nil
	}
	b, err := r.client.HGet(ctx, "idempotency:"+idempotencyKey, "value").Bytes()
	if err != nil {
		return t, false, err
	}
	if err := json.Unmarshal(b, &t); err != nil {
		return t, false, err
	}
	return t, true, nil
}

func (r *Redis[T]) Store(ctx context.Context, idempotencyKey string, value T) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, "idempotency:"+idempotencyKey, "value", b).Err()
}

func main() {
	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	rdbClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", 
		DB:       0, 
	})

	shippingIdempotency := Redis[ShippingOrder]{client: rdbClient}
	dsn := "host=localhost user=username password=password dbname=database-name port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	db.AutoMigrate(&ShippingOrder{})

	shippingRepository := NewShipping(db)

	e.POST("/shipping/order", func(c echo.Context) error {
		var request PlaceShippingOrderRequest
		if err := c.Bind(&request); err != nil {
			return err
		}
		stored, has, err := shippingIdempotency.Start(context.Background(), request.OrderID)
		if err != nil {
			return err
		}
		if has {
			return c.JSON(200, stored)
		}
		<-time.After(time.Second * 2)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		createdOrder, err := shippingRepository.Save(ctx, ShippingOrder{
			OrderID: request.OrderID,
			Vendor:  request.Vendor,
			Address: request.Address,
		})
		if err := shippingIdempotency.Store(context.Background(), createdOrder.OrderID, createdOrder); err != nil {
			return err
		}
		if err != nil {
			return err
		}
		return c.JSON(201, map[string]any{
			"ok":          true,
			"shipping_id": createdOrder.ID,
		})
	})

	e.Logger.Fatal(e.Start(":1323"))
}
