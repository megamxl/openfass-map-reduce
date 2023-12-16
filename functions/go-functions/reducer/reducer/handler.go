package function

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type CONT struct {
	BucketName   string `json:"bucketName"`
	Key          string `json:"key"`
	OutputBucket string `json:"outputBucket"`
}

type ValueData struct {
	Value int `json:"value"`
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
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	var amount int
	startTime := time.Now()

	// Listing objects
	objectCh := minioClient.ListObjects(context.TODO(), cc.BucketName, minio.ListObjectsOptions{
		Prefix:    "key/" + cc.Key,
		Recursive: true,
	})

	// Iterating over the listed objects
	for object := range objectCh {
		if object.Err != nil {
			log.Fatalln(object.Err)
		}

		// Getting object data
		object, err := minioClient.GetObject(context.TODO(), cc.BucketName, object.Key, minio.GetObjectOptions{})
		if err != nil {
			log.Fatalln(err)
		}
		data, err := ioutil.ReadAll(object)
		if err != nil {
			log.Fatalln(err)
		}

		var line ValueData
		err = json.Unmarshal(data, &line)
		if err != nil {
			log.Fatalln(err)
		}

		amount += line.Value
	}

	// Creating output object
	outputData, err := json.Marshal(map[string]int{cc.Key: amount})
	if err != nil {
		log.Fatalln(err)
	}
	_, err = minioClient.PutObject(context.TODO(), cc.OutputBucket, cc.Key, bytes.NewReader(outputData), int64(len(outputData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		log.Fatalln(err)
	}

	endTime := time.Now()

	// Preparing the response
	response := map[string]interface{}{
		"key":  cc.Key,
		"time": endTime.Sub(startTime).Seconds(),
	}
	responseData, err := json.Marshal(response)
	if err != nil {
		log.Fatalln(err)
	}

	return fmt.Sprintf(string(responseData))
}
