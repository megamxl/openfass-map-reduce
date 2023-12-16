package function

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type CONT struct {
	BucketName   string `json:"bucketName"`
	Key          string `json:"key"`
	OutputBucket string `json:"outputBucket"`
}

// Handle a serverless request
func Handle(req []byte) string {

	var cc CONT
	var err error
	err = json.Unmarshal(req, &cc)
	if err != nil {
		fmt.Println(err)
	}

	endpoint := "192.168.178.250:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalln(err)
	}

	obj, err := minioClient.GetObject(context.TODO(), cc.BucketName, cc.Key, minio.GetObjectOptions{})
	if err != nil {
		panic(err)
	}
	defer obj.Close()

	data, err := ioutil.ReadAll(obj)
	if err != nil {
		panic(err)
	}

	var lines []string
	if err := json.Unmarshal(data, &lines); err != nil {
		panic(err)
	}

	customers := make(map[string]int)
	startTime := time.Now()

	for _, line := range lines {
		lineParts := strings.Split(line, ",")
		if len(lineParts) == 8 {
			currCustomer := lineParts[6]
			if currCustomer == "" {
				continue
			}
			customers[currCustomer]++
		}
	}

	for key, value := range customers {
		assembledKey := "/key/" + key + "/" + randomString(10)
		valueJSON, err := json.Marshal(map[string]int{"value": value})
		if err != nil {
			panic(err)
		}

		_, err = minioClient.PutObject(context.TODO(), cc.OutputBucket, assembledKey, bytes.NewReader(valueJSON), int64(len(valueJSON)), minio.PutObjectOptions{ContentType: "application/json"})
		if err != nil {
			panic(err)
		}
	}

	endTime := time.Now()
	result := map[string]interface{}{
		"key":  cc.Key,
		"time": endTime.Sub(startTime).Seconds(),
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf(string(resultJSON))
}

func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	rand.Seed(time.Now().UnixNano())
	s := make([]rune, length)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
