package cache

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/go-playground/validator"
	_ "github.com/lib/pq"
)

type Customer struct {
	Name    string `json:"name" validate:"required"`
	Phone   string `json:"phone" validate:"required"`
	Zip     string `json:"zip" validate:"required"`
	City    string `json:"city" validate:"required"`
	Address string `json:"address" validate:"required"`
	Region  string `json:"region" validate:"required"`
	Email   string `json:"email" validate:"required"`
}

type Payment struct {
	Transaction   string `json:"transaction" validate:"required"`
	Requset_id    string `json:"requset_id"`
	Currency      string `json:"currency" validate:"required"`
	Provider      string `json:"provider" validate:"required"`
	Amount        int    `json:"amount" validate:"required"`
	Payment_dt    int64  `json:"payment_dt" validate:"required"`
	Bank          string `json:"bank" validate:"required"`
	Delivery_cost int    `json:"delivery_cost" validate:"required"`
	Goods_total   int    `json:"goods_total" validate:"required"`
	Custom_fee    int    `json:"custom_fee"`
}

type Item struct {
	Chrt_id      int64  `json:"chrt_id" validate:"required"`
	Track_number string `json:"track_number" validate:"required"`
	Price        int    `json:"price" validate:"required"`
	Rid          string `json:"rid" validate:"required"`
	Name         string `json:"name" validate:"required"`
	Sale         int    `json:"sale" validate:"required"`
	Size         string `json:"size" validate:"required"`
	Total_price  int    `json:"total_price" validate:"required"`
	Nm_id        int64  `json:"nm_id" validate:"required"`
	Brand        string `json:"brand" validate:"required"`
	Status       int    `json:"status" validate:"required"`
}

type Order struct {
	Order_uid          string   `json:"order_uid" validate:"required"`
	Track_number       string   `json:"track_number" validate:"required"`
	Entry              string   `json:"entry" validate:"required"`
	Delivery           Customer `json:"delivery" validate:"required"`
	Payment            Payment  `json:"payment" validate:"required"`
	Items              []Item   `json:"items" validate:"required"`
	Locale             string   `json:"locale" validate:"required"`
	Internal_signature string   `json:"internal_signature"`
	Customer_id        string   `json:"customer_id" validate:"required"`
	Delivery_service   string   `json:"delivery_service" validate:"required"`
	Shardkey           string   `json:"shardkey" validate:"required"`
	Sm_id              int      `json:"sm_id" validate:"required"`
	Date_created       string   `json:"date_created" validate:"required"`
	Oof_shard          string   `json:"oof_shard" validate:"required"`
}

var (
	cacheOrders []Order
	mutex       sync.Mutex
)

const (
	sqlDriver = "postgres"
	sqlInfo   = "host=::1 port=5432 user=postgres password=1111 dbname=wb_db sslmode=disable"

	qrCreateItem = `INSERT INTO item (chrt_id, track_number, price, rid,
					name, sale, size, total_price, nm_id, brand, status)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (chrt_id) DO NOTHING`

	qrCreatePayment = `INSERT INTO payment (transaction, request_id, currency, provider,
			  			amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (transaction) DO NOTHING`

	qrCreateCustomer = `INSERT INTO customer (name, phone, zip, city, address, region, email)
						VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (name) DO NOTHING`

	qrCreateOrder = `INSERT INTO "order" (order_uid, track_number, entry, delivery, payment, item,
		 			 locale, internal_signature, customer_id, delivery_service,
		 			 shardkey, sm_id, date_created, oof_shard, item_amount)
					 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 1)
					 ON CONFLICT (order_uid, item) DO UPDATE SET item_amount = "order".item_amount + 1`

	qrCreateOrderJSON = `INSERT INTO order_json (order_uid, json)
						 VALUES ($1, $2) ON CONFLICT (order_uid) DO NOTHING`
)

func GetStringOrderData(order_uid *string) string {
	var strOrderData string
	mutex.Lock()
	for _, order := range cacheOrders {
		if order.Order_uid == *order_uid {
			orderData, err := json.MarshalIndent(order, "", "\t")
			strOrderData = string(orderData)
			if err != nil {
				fmt.Println(err)
				mutex.Unlock()
				return ""
			}
		}
	}
	mutex.Unlock()
	return strOrderData
}

func OrdersToCache() {
	log.Printf("Updating orders in cache from DB")

	// Connect to PostgreSQL server
	db, err := sql.Open(sqlDriver, sqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Get all previos JSON from BD
	queryResult, err := db.Query("SELECT json FROM order_json")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Record query results to cacheOrders
	for queryResult.Next() {
		var orderJSON []byte
		if err := queryResult.Scan(&orderJSON); err != nil {
			fmt.Println(err)
			return
		}
		var order Order
		if err := json.Unmarshal(orderJSON, &order); err != nil {
			fmt.Println(err)
			return
		}
		cacheOrders = append(cacheOrders, order)
	}

	log.Printf("%d orders successfully downloaded in cache from DB", len(cacheOrders))
}

func RecordNewOrder(data *[]byte) {
	var order Order

	// Unmarshaling JSON to Order struct
	if err := json.Unmarshal(*data, &order); err != nil {
		fmt.Println(err)
		return
	}

	// Validating Order struct
	validate := validator.New()
	if err := validate.Struct(order); err != nil {
		fmt.Println(err)
		return
	}

	// Validating every Item struct in []Items
	for _, item := range order.Items {
		if err := validate.Struct(item); err != nil {
			fmt.Println(err)
			return
		}
	}

	// Connect PostgreSQL server
	db, err := sql.Open(sqlDriver, sqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create new Customer in DB
	_, err = db.Exec(qrCreateCustomer, order.Delivery.Name, order.Delivery.Phone,
		order.Delivery.Zip, order.Delivery.City, order.Delivery.Address,
		order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Create new Payment in DB
	_, err = db.Exec(qrCreatePayment, order.Payment.Transaction, order.Payment.Requset_id,
		order.Payment.Currency, order.Payment.Provider, order.Payment.Amount,
		order.Payment.Payment_dt, order.Payment.Bank, order.Payment.Delivery_cost,
		order.Payment.Goods_total, order.Payment.Custom_fee)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Create new Items and Order in DB with primary keys order.Order_uid and item.Chrt_id
	for _, item := range order.Items {
		_, err = db.Exec(qrCreateItem, item.Chrt_id, item.Track_number, item.Price, item.Rid,
			item.Name, item.Sale, item.Size, item.Total_price, item.Nm_id, item.Brand, item.Status)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err = db.Exec(qrCreateOrder, order.Order_uid, order.Track_number, order.Entry, order.Delivery.Name,
			order.Payment.Transaction, item.Chrt_id, order.Locale, order.Internal_signature,
			order.Customer_id, order.Delivery_service, order.Shardkey, order.Sm_id, order.Date_created, order.Oof_shard)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	// Create new orderJSON in DB
	orderJSON, err := json.Marshal(order)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = db.Exec(qrCreateOrderJSON, order.Order_uid, orderJSON)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Add new order to cache
	mutex.Lock()
	cacheOrders = append(cacheOrders, order)
	mutex.Unlock()
}
