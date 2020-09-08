package mysql-driver

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"strings"
)

/*type SetField struct {
	FieldName string
	FieldData interface{}
}
type SqlValues []SetField
*/
type DataStruct map[string]interface{}
type DbConfig struct {
	Db           *sql.DB
	DriverName   string
	Addr         string
	User         string
	Passwd       string
	Port         string
	DBName       string
	MaxOpenConns int
	MaxIdleConns int
	Debug        bool
}

func (config *DbConfig) Connect() (err error) {
	cfg := mysql.NewConfig()
	cfg.User = config.User
	cfg.Passwd = config.Passwd
	cfg.Net = "tcp"
	cfg.Addr = config.Addr
	cfg.DBName = config.DBName
	dsn := cfg.FormatDSN()
	config.Db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	if err := config.Db.Ping(); err != nil {
		return err
	}
	maxOpenConns := 0
	if config.MaxOpenConns > 0 {
		maxOpenConns = config.MaxOpenConns
	}
	maxIdleConns := 0
	if config.MaxIdleConns > 0 {
		maxIdleConns = config.MaxIdleConns
	}
	config.Db.SetMaxOpenConns(maxOpenConns)
	config.Db.SetMaxIdleConns(maxIdleConns)
	return nil
}

func (S *DataStruct) parseData() (string, []interface{}, error) {
	keys := []string{}
	values := []interface{}{}
	for key, value := range *S {
		keys = append(keys, key)
		values = append(values, value)
	}
	return strings.Join(keys, ","), values, nil
}

//添加或者修改数据
func (d *DataStruct) Set(key string, value interface{}) {
	(*d)[key] = value
}

//获取数据
func (d DataStruct) Get(key string) interface{} {
	return d[key]
}

//配合update使用，生成 field=?
func (S *DataStruct) setData() (string, []interface{}, error) {
	keys := []string{}
	values := []interface{}{}
	for key, value := range *S {
		keys = append(keys, key+"=?")
		values = append(values, value)
	}
	return strings.Join(keys, ","), values, nil
}

//插入数据
func (config *DbConfig) Insert(table string, datas DataStruct) (id int64, err error) {
	s, v, _ := datas.parseData()
	placeString := fmt.Sprintf("%s", strings.Repeat("?,", len(v)))
	placeString = placeString[:len(placeString)-1]
	sqlString := "INSERT INTO `" + table + "` (" + s + ") VALUES (" + placeString + ")"
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", v)
	}
	result, err := config.Db.Exec(sqlString, v...)
	if err != nil {
		return
	}
	id, err = result.LastInsertId()
	if err != nil {
		return
	}
	return
}

//更新
func (config *DbConfig) Update(table string, datas DataStruct, where string, args ...interface{}) (num int64, err error) {
	s, v, _ := datas.setData()
	sqlString := "UPDATE `" + table + "` SET " + s
	if where != "" {
		sqlString += " WHERE " + where
	}
	for _, value := range args {
		v = append(v, value)
	}
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", v)
	}
	result, err := config.Db.Exec(sqlString, v...)
	if err != nil {
		return
	}
	num, err = result.RowsAffected()
	return
}

//获取一条
func (config *DbConfig) GetOne(table, fields, where string, args ...interface{}) (map[string]interface{}, error) {
	sqlString := "SELECT " + fields + " FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	sqlString += " LIMIT 0,1"
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", args)
	}
	rows, err := config.Db.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	item := make(map[string]interface{})
	for rows.Next() {
		_ = rows.Scan(cache...)
		for i, data := range cache {
			item[columns[i]] = *(data.(*interface{})) //取实际类型
		}
	}
	return item, nil
}

//批量查询，不带分页计算
func (config *DbConfig) Select(table string, fields string, where string, args ...interface{}) ([]map[string]interface{}, error) {
	sqlString := "SELECT " + fields + " FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", args)
	}
	rows, err := config.Db.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	var results []map[string]interface{}
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *(data.(*interface{})) //取实际类型
		}
		results = append(results, item)
	}
	return results, nil
}
func (config *DbConfig) Delete(table string, where string, args ...interface{}) (num int64, err error) {
	sqlString := "DELETE FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", args)
	}
	stmt, err := config.Db.Prepare(sqlString)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	num, err = result.RowsAffected()
	return
}

func (config *DbConfig) Count(table string, where string, args ...interface{}) (total int64, err error) {
	sqlString := "SELECT COUNT(*) as total FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", args)
	}
	stmt, err := config.Db.Prepare(sqlString)
	if err != nil {
		return
	}
	row := stmt.QueryRow(args...)
	err = row.Scan(&total)
	return
}

func (config *DbConfig) Close() error {
	err := config.Db.Close()
	return err
}

func Format2String(datas map[string]interface{}, key string) string {
	if datas[key] == nil {
		return ""
	}
	ba := []byte{}
	data := datas[key].([]uint8)
	for _, b := range data {
		ba = append(ba, byte(b))
	}
	return string(ba)
}

func (config *DbConfig) BatchInsert(table string, datas []DataStruct) (num int64, err error) {
	var (
		placeString string
		columnName  []string
		sqlColumn string
		columnData  []interface{}
	)
	if table == "" || len(datas) == 0 {
		return 0, errors.New("Param ERROR")
	}
	if len(datas) == 1 {
		_, err := config.Insert(table, datas[0])
		if err != nil {
			return 0, err
		}
		return 1, nil
	}
	s := strings.Repeat("?,", len(datas[0]))
	for _, data := range datas {
		placeString += fmt.Sprintf("(%s),", strings.TrimSuffix(s, ","))
		if columnName == nil {
			for k :=range  data {
				columnName = append(columnName, k)
			}
		}
		for _, key := range  columnName {
			columnData = append(columnData, data[key])
		}
		sqlColumn = strings.Join(columnName, ",")
	}
	sqlString := fmt.Sprintf("INSERT INTO `%s`(%s) values %s", table, sqlColumn, strings.TrimSuffix(placeString, ","))
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", columnData)
	}
	res, err := config.Db.Exec(sqlString, columnData...)
	if err != nil {
		return
	}
	num, err = res.RowsAffected()
	if err != nil {
		return
	}
	return
}
func (config *DbConfig) Query(sqlString string, args ...interface{}) ([]map[string]interface{}, error) {
	if config.Debug {
		fmt.Println("SQL Debug:", sqlString,"\nSQL Param:", args)
	}
	rows, err := config.Db.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	var results []map[string]interface{}
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *(data.(*interface{})) //取实际类型
		}
		results = append(results, item)
	}
	return results, nil
}
