package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	storage "github.com/Azure/azure-sdk-for-go/storage"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	requestTimeoutInSeconds = 60
	version                 = "0.0.0"
	charsetForBlobName      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charsetForContainerName = "abcdefghijklmnopqrstuvwxyz0123456789"
)

var (
	accountName = kingpin.Flag("AccountName", "AccountName").Required().String()
	accessKey   = kingpin.Flag("AccessKey", "AccessKey").Required().String()
	// "redmond.ext-n35r0906.masd.stbtest.microsoft.com"
	baseServiceURL = kingpin.Flag("BaseServiceURL", "BaseServiceURL").Required().String()
	apiVersion     = kingpin.Flag("ApiVersion", "ApiVersion").Default("2015-04-05").String()
	blobNamePrefix = kingpin.Flag("BlobNamePrefix", "BlobNamePrefix").Default("python").String()
	threadCount    = kingpin.Flag("ThreadCount", "ThreadCount").Default("10").Int()
	dataLengthInKB = kingpin.Flag("DataLengthInKB", "DataLengthInKB").Default("10").Int()
)

type OperationResult struct {
	locker                 sync.Mutex
	Duration               time.Duration
	SuccessfulThreadCounts int
	FailedThreadCounts     int
}

func upload(blobClient *storage.BlobStorageClient, threadId int, containerName string, blobName string, startFlag chan struct{}, wg *sync.WaitGroup, data *[]byte, operationResult *OperationResult) {
	<-startFlag
	log.Printf("Start uploading %v in the thread %v\n", blobName, threadId)
	container := blobClient.GetContainerReference(containerName)
	blob := container.GetBlobReference(blobName)
	startTime := time.Now()
	err := blob.CreateBlockBlobFromReader(bytes.NewReader(*data), nil)
	elapsedTime := time.Since(startTime)
	operationResult.locker.Lock()
	defer operationResult.locker.Unlock()
	if err != nil {
		log.Printf("Error when uploading %v in the thread %v: %v\n", blobName, threadId, err)
		operationResult.FailedThreadCounts++
	} else {
		operationResult.SuccessfulThreadCounts++
		operationResult.Duration = operationResult.Duration + elapsedTime
		log.Printf("Finish uploading %v in the thread %v successfully. Duration: %s\n", blobName, threadId, elapsedTime)
	}
	wg.Done()
}

func download() {

}

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charsetForBlobName)
}

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	if *dataLengthInKB > 256*1024 {
		panic(fmt.Sprintf("DataLengthInKB cannot be larger than 256 MiB in this program. You set it to %v", *dataLengthInKB))
	}

	client, err := storage.NewClient(*accountName, *accessKey, *baseServiceURL, *apiVersion, true)
	if err != nil {
		log.Printf("Cannot create the storage client: %v\n", err)
		return
	}
	log.Printf("Connect to AzureStack storage successfully\n")
	service := client.GetBlobService()

	containerName := *blobNamePrefix + StringWithCharset(20, charsetForContainerName)
	container := service.GetContainerReference(containerName)
	createOptions := storage.CreateContainerOptions{Timeout: requestTimeoutInSeconds}
	err = container.Create(&createOptions)
	if err != nil {
		if !strings.Contains(err.Error(), "ContainerAlreadyExists") {
			log.Printf("Cannot create the container %v: %v\n", containerName, err)
			return
		} else {
			log.Printf("The container %v exists and reuse it\n", containerName)
		}
	} else {
		log.Printf("Create the container %v on AzureStack storage successfully\n", containerName)
	}

	operationResult := OperationResult{
		locker:                 sync.Mutex{},
		Duration:               0,
		SuccessfulThreadCounts: 0,
		FailedThreadCounts:     0,
	}
	data := make([]byte, *dataLengthInKB)
	rand.Read(data)
	startFlag := make(chan struct{})
	var wg sync.WaitGroup
	for i := 1; i <= *threadCount; i++ {
		blobName := *blobNamePrefix + String(25)
		wg.Add(1)
		go upload(&service, i, containerName, blobName, startFlag, &wg, &data, &operationResult)
	}
	close(startFlag)
	wg.Wait()
	log.Println("All upload threads finished")
	log.Printf("Successful threads: %v\n", operationResult.SuccessfulThreadCounts)
	log.Printf("Failed threads: %v\n", operationResult.FailedThreadCounts)
	log.Printf("Average upload time: %v nanoseconds\n", operationResult.Duration.Nanoseconds()/int64(operationResult.SuccessfulThreadCounts))

	deleteOptions := storage.DeleteContainerOptions{Timeout: requestTimeoutInSeconds}
	err = container.Delete(&deleteOptions)
	if err != nil {
		log.Printf("Cannot delete the container %v: %v\n", containerName, err)
		return
	}
	log.Printf("Delete the container %v on AzureStack storage successfully\n", containerName)
}
