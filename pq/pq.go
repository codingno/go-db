package pq

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type Postgres struct {
	db *sql.DB
}

func New() Postgres {

	godotenv.Load()

	DB_HOST := os.Getenv("DB_HOST")
	DB_PORT := os.Getenv("DB_PORT")
	DB_USER := os.Getenv("DB_USER")
	DB_PASSWORD := os.Getenv("DB_PASSWORD")
	DB_NAME := os.Getenv("DB_NAME")
	DB_DRIVER := "postgres"

	if DB_HOST == "" || DB_PORT == "" || DB_USER == "" || DB_PASSWORD == "" || DB_NAME == "" {
		panic("Missing required environment variables DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
	}

	url := fmt.Sprintf("%s://%s:%s@%s:%s/%s?sslmode=disable", DB_DRIVER, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

	conn, err := sql.Open(DB_DRIVER, url)

	if err != nil {
		panic(err)
	}

	return Postgres{conn}
}

func (p *Postgres) DB() *sql.DB {
	return p.db
}

func (p *Postgres) Row(q string, fields any, params ...any) error {

	value := reflect.ValueOf(fields)
	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return errors.New("fields must be a pointer to a struct")
	}

	structValue := value.Elem()
	numFields := structValue.NumField()

	rows, err := p.db.Query(q, params...)
	if err != nil {
		log.Println("error query", err)
		return err
	}

	columNames, _ := rows.Columns()
	values := make([]any, numFields)
	resultValue := make(map[string]any)

	for rows.Next() {

		for i := 0; i < numFields; i++ {
			types := structValue.Field(i).Type()
			newValue := reflect.New(types).Interface()
			values[i] = &newValue
		}

		err = rows.Scan(values...)
		if err != nil {
			log.Println("error scan", err)
			return err
		}

		for i, v := range columNames {

			switch reflect.ValueOf(values[i]).Elem().Interface().(type) {
			case []byte:
				resultValue[v] = string(reflect.ValueOf(values[i]).Elem().Interface().([]byte))
			default:

				if structValue.Field(i).Type().Name() == "bool" {
					val := reflect.ValueOf(values[i]).Elem().Interface().(int64)
					resultValue[v] = val != 0
					continue
				}
				resultValue[v] = values[i]
			}
		}
	}

	jsonStr, err := sonic.Marshal(resultValue)
	if err != nil {
		return err
	}

	err = sonic.Unmarshal(jsonStr, fields)
	if err != nil {
		return err
	}

	return nil
}

func (p *Postgres) Query(q string, fields any, params ...any) error {

	valueAny := reflect.ValueOf(fields)

	if valueAny.Kind() != reflect.Ptr {
		return errors.New("fields must be a pointer to a struct")
	}

	valueArray := reflect.ValueOf(valueAny.Elem().Interface()).Elem()
	if valueArray.Kind() != reflect.Slice {
		return errors.New("fields must be an array")
	}

	valueNew := reflect.New(valueArray.Type().Elem()).Interface()

	value := reflect.ValueOf(valueNew)
	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return errors.New("fields must be a pointer to a struct")
	}

	structValue := value.Elem()
	numFields := structValue.NumField()

	rows, err := p.db.Query(q, params...)

	if err != nil {
		log.Println("error query", err)
		return err
	}

	columNames, _ := rows.Columns()

	values := make([]any, numFields)
	resultValues := []map[string]any{}
	for rows.Next() {
		resultValue := make(map[string]any)
		for i := 0; i < numFields; i++ {
			types := structValue.Field(i).Type()
			newValue := reflect.New(types).Interface()
			values[i] = &newValue
		}

		err = rows.Scan(values...)
		if err != nil {
			log.Println("error scan", err)
			return err
		}

		for i, v := range columNames {
			switch reflect.ValueOf(values[i]).Elem().Interface().(type) {
			case []byte:
				resultValue[v] = string(reflect.ValueOf(values[i]).Elem().Interface().([]byte))
			default:

				if structValue.Field(i).Type().Name() == "bool" {
					val := reflect.ValueOf(values[i]).Elem().Interface().(int64)
					resultValue[v] = val != 0
					continue
				}

				resultValue[v] = values[i]
			}
		}

		resultValues = append(resultValues, resultValue)
	}

	jsonStr, _ := sonic.Marshal(resultValues)
	sonic.Unmarshal(jsonStr, fields)

	return nil
}

func (p *Postgres) InsertOrUpdateStruct(tableName string, data any, params ...any) (sql.Result, error) {

	val := reflect.ValueOf(data).Elem()
	typ := val.Type()
	numFields := typ.NumField()

	var prevVal reflect.Value
	var prevTyp reflect.Type

	if len(params) > 0 {
		prevVal = reflect.ValueOf(params[0]).Elem()
		prevTyp = prevVal.Type()
	}

	query := fmt.Sprintf("INSERT INTO %s (", tableName)
	value := make([]any, 0)
	values := "VALUES ("
	update := ""
	idValue, err := getFieldValueByJSONTag(data, "id")

	if err != nil {
		return nil, err
	}

	notNilFields := countNotNilFields(data)

	for i := 0; i < numFields; i++ {
		field := typ.Field(i)
		kind := val.FieldByName(field.Name).Kind()
		valueField := val.FieldByName(field.Name)

		if kind == reflect.Ptr && valueField.IsNil() {
			if len(params) == 0 {
				continue
			}

			prevField := prevTyp.Field(i)
			prevValue := prevVal.FieldByName(prevField.Name).Interface()
			currentValue := val.FieldByName(field.Name).Interface()

			if prevValue == currentValue {
				continue
			}
		}

		columnName := strings.Split(field.Tag.Get("json"), ",")[0]

		if columnName == "id" {
			if valueField.Interface() == 0 {
				continue
			}
		}

		query += columnName + ","

		percentNum := i + 1
		allField := notNilFields + percentNum + 1

		if reflect.ValueOf(idValue).IsNil() {
			percentNum = i
			allField = notNilFields + percentNum + 1
		}

		values += fmt.Sprintf("$%d,", percentNum)
		if len(update) == 0 {
			update += "ON CONFLICT (id) DO UPDATE SET " + columnName + " = $" + fmt.Sprintf("%d", allField)
		} else {
			update += ", " + columnName + " = $" + fmt.Sprintf("%d", allField)
		}
		value = append(value, valueField.Interface())

	}

	query = query[:len(query)-1] + ")"
	values = values[:len(values)-1] + ")"
	value = append(value, value...)
	stmt := query + " " + values + " " + update

	returnData, err := p.db.Exec(stmt, value...)
	if err != nil {
		return nil, err
	}

	id, _ := returnData.LastInsertId()
	idInt := int(id)
	reflect.ValueOf(data).Elem().FieldByName("ID").Set(reflect.ValueOf(&idInt))

	return returnData, err
}

func getFieldValueByJSONTag(obj interface{}, jsonTag string) (interface{}, error) {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		tag := field.Tag.Get("json")
		if tag == jsonTag {
			return val.Field(i).Interface(), nil
		}

		if field.Type.Kind() == reflect.Struct {
			if nestedVal, err := getFieldValueByJSONTag(val.Field(i).Interface(), jsonTag); err == nil {
				return nestedVal, nil
			}
		}
	}

	return nil, fmt.Errorf("field with JSON tag %s not found", jsonTag)
}

func countNotNilFields(obj interface{}) int {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	count := 0

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			count++
		} else if field.Kind() == reflect.Struct {
			count += countNotNilFields(field.Interface())
		}
	}

	return count
}

func (p *Postgres) SqlExec(query string, name string, params ...any) (map[string]any, sql.Result) {

	data, err := p.db.Exec(query, params...)
	var id int64 = 0

	if err != nil {
		fmt.Println(err.Error())
		result := map[string]interface{}{
			"id":      id,
			"message": name + " failed",
		}
		return result, data
	} else {
		id, _ = data.LastInsertId()
	}

	result := map[string]interface{}{
		"id":      id,
		"message": name + " success",
	}
	return result, data

}
