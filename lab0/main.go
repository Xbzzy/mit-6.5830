package main

import (
	"fmt"
	"main/handlers"
	"net/http"
)

func main() {
	// Fill out the HomeHandler function in handlers/handlers.go which handles the user's GET request.
	// Start an http server using http.ListenAndServe that handles requests using HomeHandler.

	err := http.ListenAndServe(":8080", http.HandlerFunc(handlers.HomeHandler))
	if err != nil {
		fmt.Println("web listen failed err:", err)
	}
}
