package pgx

import (
	"fmt"
	"testing"
)

func Test_batchwhere(t *testing.T) {

	data := []string{"a", "b", "c", "d"}

	sql := SQLStr("select * from table where a= 1").AddParams().AndIn("key", data).OrderBy("key asc").Limit(100).Offset(12)

	fmt.Println(sql.Info())

}
