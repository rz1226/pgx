package pgx

/**
type Tai struct {
	Id          int64   `orm:"id" auto:"1""`
	Name        string  `orm:"name"`
	Age         int64   `orm:"age"`
	Weight      float64 `orm:"weight"`
	Create_time string  `orm:"create_time" auto:"1"`
	Some        string
}
type  UserTag struct{
	UserId string `orm:"user_id"`
	Phone string `orm:"phone"`

}

func main(){
	sqlstr := "select user_id,phone  from dev_user_tag limit 10"
	var u  []*UserTag
	res, err := pgx.SQLStr(sqlstr).Query( Kit)
	if err != nil {
		fmt.Println("err=", err )
	}
	fmt.Println("res=",res )
	err  = res.ToStruct(&u )
	if err != nil {
		fmt.Println("err=", err )
	}
	for k, v := range u {
		fmt.Println("id=", k, "  v=", *v)
	}

}



}
func testone() {
	sqlstr := "select * from tai"
	u := new(repo.Tai )
	res, err := mysqlx.SqlStr(sqlstr).AddParams().Query(repo.Kit)
	res.ToStruct( u)
	fmt.Println(res)
	fmt.Println("data=", u)

	m := mysqlx.NewBM(u ).ToMap()
	fmt.Println("m==",m)

	var d []*repo.Tai
	mysqlx.Map2Struct(m,&d )
	fmt.Println(mysqlx.NewBM(&d).ToSQLInsert("x"))


	// sql, err := mysqlx.NewBM(u).ToSQLInsert("tai")
	// fmt.Println(sql.Info(), err)


	// sql2, err := mysqlx.NewBM(u).ToSQLUpdate("tai", nil,"")
	// fmt.Println(sql2.Info(), err)


	// condition := mysqlx.SQLStr(" where id =? and name =?").AddParams(2,"mike")
	// sql3 := sql2.ConcatSQL(condition )
	// fmt.Println(sql3.Info())

}
var MYSQL_HOST = strings.TrimSpace(os.Getenv("XX_MYSQL_HOST"))
var MYSQL_PORT = strings.TrimSpace(os.Getenv("XX_MYSQL_PORT"))
var MYSQL_USERNAME = strings.TrimSpace(os.Getenv("XX_MYSQL_USERNAME"))
var MYSQL_PASSWORD = strings.TrimSpace(os.Getenv("XX_MYSQL_PASSWORD"))
var HBASE_HOST = strings.TrimSpace(os.Getenv("XX_HBASE_HOST"))
var HBASE_USER = strings.TrimSpace(os.Getenv("XX_HBASE_USER"))
var HBASE_PASS = strings.TrimSpace(os.Getenv("XX_HBASE_PASS"))




var Kit *mysqlx.DB


func init() {
	dbconf := mysqlx.NewDBConf(conf.MYSQL_USERNAME, conf.MYSQL_PASSWORD, conf.MYSQL_HOST, conf.MYSQL_PORT, "tai", 4)
	kit, err := dbconf.Connect()

	if err != nil {
		fmt.Println(dbconf.Str(), err)
		panic("no db ")
	}
	Kit = kit

	mysqlx.Conf.TagName = "orm"

	f := func(tags reflect.StructTag) bool {
		tag := tags.Get("auto")
		if tag == "1" {
			return true
		}
		return false
	}
	mysqlx.Conf.FuncAuto = f

}

*/
