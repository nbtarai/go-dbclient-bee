package db

import (
	"fmt"
	"net/url"
)

func MySQLDsn(username string, password string, host string, dbname string) string {
	q := url.Values{}
	q.Set("charset", "utf8mb4")
	q.Set("parseTime", "True")
	q.Set("loc", "Asia/Tokyo")

	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", username, password, host, dbname, q.Encode())
}
