package pool

import (
	"encoding/json"
	"log"
	"net/http"
)

// WriteJSON writes the object to the ResponseWriter. If the encoding fails, an
// error is written instead. The Content-Type of the response header is set
// accordingly.
func WriteJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(obj); err != nil {
		log.Println("failed to encode api response, err: ", err.Error())
	}
}
