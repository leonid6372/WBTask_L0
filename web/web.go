package web

import (
	"fmt"
	"html/template"
	"log"
	"net/http"

	"main.go/cache"
)

// Handler for homepage
func home(w http.ResponseWriter, r *http.Request) {
	// Read homepage template
	ts, err := template.ParseFiles("./web/home.page.tmpl")
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "Internal Server Error", 500)
		return
	}

	// Sending tamplate as respons
	err = ts.Execute(w, nil)
	if err != nil {
		log.Println(err.Error())
		http.Error(w, "Internal Server Error", 500)
	}
}

// Handler for showing order data
func search(w http.ResponseWriter, r *http.Request) {
	// Get order_uid from URL
	order_uid := r.URL.Query().Get("order_uid")
	// Search order_uid in cacheOrders
	orderData := cache.GetStringOrderData(&order_uid)
	if orderData != "" {
		fmt.Fprintf(w, orderData)
	} else {
		fmt.Fprintf(w, "Not found order with entered order_uid")
	}
}

func StartWebServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", home)
	mux.HandleFunc("/search", search)

	log.Println("Starting http-server http://127.0.0.1:80/")
	err := http.ListenAndServe(":80", mux)
	log.Fatal(err)
}
