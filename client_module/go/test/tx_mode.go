package test

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	smartclient "smartresource/clientmodule/go/rdb"
)

func TestTxCase1() (*smartclient.Records, error) {

	txClient, err := smartclient.NewTx("crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	//defer txClient.Close()

	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr
	email := "john@example.com_alter_" + idStr

	params := smartclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
		Add(userId, smartclient.ValueType_STRING).
		Add(email, smartclient.ValueType_STRING)
	_, err = txClient.Execute(
		"UPDATE users SET email = $2 WHERE id = $1",
		params,
	)

	params = smartclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
		Add(userId, smartclient.ValueType_STRING)
	records, _ := txClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	dbClient := smartclient.Get("crm-system")
	records2, _ := dbClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email mismatch: %w", err)
	}

	err = txClient.Commit()
	records3, _ := dbClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	log.Println(records.Get(0).Get("email"), records2.Get(0).Get("email"), records3.Get(0).Get("email"))

	return records, nil
}

func TestTxCase2() (*smartclient.Records, error) {

	txClient, err := smartclient.NewTx("crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	//defer txClient.Close()

	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr
	email := "john@example.com_alter_" + idStr

	params := smartclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
		Add(userId, smartclient.ValueType_STRING).
		Add(email, smartclient.ValueType_STRING)
	_, err = txClient.Execute(
		"UPDATE users SET email = $2 WHERE id = $1",
		params,
	)

	params = smartclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
		Add(userId, smartclient.ValueType_STRING)
	records, _ := txClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	dbClient := smartclient.Get("crm-system")
	records2, _ := dbClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	if records.Get(0).Get("email") != email {
		return nil, fmt.Errorf("email mismatch: %w", err)
	}

	err = txClient.Rollback()
	records3, _ := dbClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	log.Println(records.Get(0).Get("email"), records2.Get(0).Get("email"), records3.Get(0).Get("email"))

	return records, nil
}

func TestTxCase3() (*smartclient.Records, error) {

	txClient, err := smartclient.NewTx("crm-system", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("NewTx error: %w", err)
	}
	//defer txClient.Close()

	id := rand.Intn(1000000)
	idStr := "0000000" + strconv.Itoa(id)
	idStr = idStr[len(idStr)-7:]
	userId := "d5794b1b-5f92-4dc6-aa48-085d_" + idStr
	email := "john@example.com_alter_" + idStr

	params := smartclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
		Add(userId, smartclient.ValueType_STRING).
		Add(email, smartclient.ValueType_STRING)
	_, err = txClient.Execute(
		"UPDATE users SET email = $2 WHERE id = $1",
		params,
	)

	time.Sleep(10 * time.Second)
	params = smartclient.NewParams().
		// Add("01916e5a-2345-7002-b000-000000000002", smartclient.ValueType_STRING)
		Add(userId, smartclient.ValueType_STRING)
	records, _ := txClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	time.Sleep(10 * time.Second)
	_, err = txClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	time.Sleep(10 * time.Second)
	_, err = txClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	time.Sleep(16 * time.Second)
	_, err = txClient.Query(
		"SELECT * FROM users where id = $1",
		params,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("Query error: %w", err)
	}

	err = txClient.Rollback()

	return records, nil
}
