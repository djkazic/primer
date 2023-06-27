package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

const (
	originalDBFile      = "/lnd/data/graph/mainnet/channel.db"
	dbFile              = "/tmp/channel.db"
	outputFileName      = "graph-001d.db"
	outputDBFile        = "/cryptpad/graph-001d.db"
	md5SumsFile         = "/cryptpad/MD5SUMS"
	graphNodeBucketName = "graph-node"
	graphEdgeBucketName = "graph-edge"
	containerName       = "recoverylnd"
)

func copyFile(src, dest string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	return destFile.Sync()
}

func main() {
	log.Println("Primer v0.1.0 starting")
	// initial run
	run()
	ticker := time.NewTicker(time.Hour)
	for {
		select {
		case <-ticker.C:
			run()
		}
	}
}

func run() {
	log.Println("Cycle start: stopping recoverylnd")
	client, err := client.NewEnvClient()
	if err != nil {
		log.Fatalf("Could not initialize docker client: %w", err)
	}
	os.Remove(dbFile)
	ctx := context.Background()
	err = client.ContainerStop(ctx, containerName, nil)
	if err != nil {
		log.Printf("Unable to stop container %s: %s", containerName, err)
	}

	log.Println("Copying sourcedb")
	err = copyFile(originalDBFile, dbFile)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	// Open the existing channel.db database file in read-only mode.
	existingDB, err := bolt.Open(dbFile, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer existingDB.Close()
	log.Println("Opened channeldb")

	// Create a new graph-001d.db database file.
	os.Remove(outputDBFile)
	strippedDBPath := outputDBFile
	strippedDB, err := bolt.Open(strippedDBPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer strippedDB.Close()
    log.Println("Created strippeddb")

	// Copy the graph-node bucket to the stripped database.
	if err := copyBucket(existingDB, strippedDB, graphNodeBucketName); err != nil {
		log.Println(err)
	}
	log.Println("Copied graph-node bucket")

	// Copy the graph-edge bucket to the stripped database.
	if err := copyBucket(existingDB, strippedDB, graphEdgeBucketName); err != nil {
		log.Println(err)
	}
	log.Println("Copied graph-edge bucket")

	log.Println("Stripped database created successfully")
	strippedDB.Close()
	err = os.Chmod(strippedDBPath, 0775)

	// Generate the MD5 checksum for the graph-001d.db file.
	err = generateMD5Checksum(strippedDBPath, md5SumsFile)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("MD5 checksum generated successfully")

	log.Println("Starting recoverylnd")
	ctx = context.Background()
	if err := client.ContainerStart(ctx, containerName, types.ContainerStartOptions{}); err != nil {
		log.Printf("Unable to stop container %s: %s", containerName, err)
	}
	time.Sleep(10 * time.Second)
	// Container should be up by now
	//TODO: check container state
}

func copyBucket(srcDB, destDB *bolt.DB, bucketName string) error {
	return srcDB.View(func(tx *bolt.Tx) error {
		srcBucket := tx.Bucket([]byte(bucketName))
		log.Printf("Copying %s", bucketName)
		if srcBucket == nil {
			log.Printf("Source bucket %s does not exist. Skipping bucket copy.", bucketName)
			return nil
		}
		return destDB.Update(func(tx *bolt.Tx) error {
			destBucket := tx.Bucket([]byte(bucketName))
			if destBucket == nil {
				var err error
				destBucket, err = tx.CreateBucket([]byte(bucketName))
				if err != nil {
					return err
				}
			}
			if err := copyNestedBucket(srcBucket, destBucket, bucketName); err != nil {
				return err
			}
			return nil
		})
	})
}

func stringInList(s string, list []string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func copyNestedBucket(srcBucket, destBucket *bolt.Bucket, bucketName string) error {
	err := srcBucket.ForEach(func(key, value []byte) error {
		if bucketName == graphEdgeBucketName || bucketName == graphNodeBucketName {
			if nestedBucket := srcBucket.Bucket(key); nestedBucket != nil {
				nestedDestBucket := destBucket.Bucket(key)
				log.Printf("Evaluating nested bucket %s", string(key))
				if string(key) == "zombie-index" {
					log.Printf("Skipping zombie-index")
					destBucket.CreateBucket([]byte("zombie-index"))
				} else if string(key) == "chan-index" {
					log.Printf("Skipping chan-index")
					destBucket.CreateBucket([]byte("chan-index"))
				} else if string(key) == "disabled-edge-policy-index" {
					log.Printf("Skipping disabled-edge-policy-index")
				} else {
					if nestedDestBucket == nil {
						var err error
						nestedDestBucket, err = destBucket.CreateBucket([]byte(key))
						log.Printf("Created nested destBucket %s", string(key))
						if err != nil {
							log.Printf("Failed to create nested bucket %s: %v", string(key), err)
							return err
						}
					}
					nestedDestBucket.FillPercent = 1.0
					if err := copyNestedBucket(nestedBucket, nestedDestBucket, bucketName); err != nil {
						log.Printf("Failed to copy nested bucket %s: %v", string(key), err)
						return err
					}
					log.Printf("Successfully copied nested bucket %s", string(key))
				}
			} else {
				// log.Printf("Evaluating %s/%x", bucketName, key)
				if (bucketName == graphEdgeBucketName) || (bucketName == graphNodeBucketName) {
					if err := destBucket.Put(key, value); err != nil {
						log.Printf("Failed to put key %s in bucket: %v", string(key), err)
						return err
					}
				} else {
					log.Printf("Skipping malformed non-subbucket in %s: %x. Len = %d", bucketName, key, len(key))
				}
			}
		}
		return nil
	})
	return err
}

func generateMD5Checksum(filename, checksumFile string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	checksum := hex.EncodeToString(hash.Sum(nil))
	md5sumsFile, err := os.Create(checksumFile)
	if err != nil {
		return err
	}
	defer md5sumsFile.Close()
	_, err = fmt.Fprintf(md5sumsFile, "%s  %s\n", checksum, outputFileName)
	if err != nil {
		return err
	}
	return nil
}
