package main

import (
	"io"
	"net/http"
)

func syncResponder() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		io.WriteString(writer, "Hey there, I'm syncing")
	}
}
