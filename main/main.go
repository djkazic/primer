package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	originalDBFile      = "/lnd/data/graph/mainnet/channel.db"
	dbFile              = "/tmp/channel.db"
	outputDBFile        = "/cryptpad/graph-001d.db"
	md5SumsFile         = "/cryptpad/MD5SUMS"
	graphNodeBucketName = "graph-node"
	graphEdgeBucketName = "graph-edge"
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
	ticker := time.NewTicker(time.Hour)

	for {
		select {
		case <-ticker.C:
			run()
		}
	}
}

func run() {
    fmt.Println("Primer v0.1.0 starting")
	fmt.Println("Copying sourcedb")
	err := copyFile(originalDBFile, dbFile)
	if err != nil {
		log.Fatal(err)
	}

	// Open the existing channel.db database file in read-only mode.
	existingDB, err := bolt.Open(dbFile, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer existingDB.Close()
        fmt.Println("Opened channeldb")

	// Create a new graph-001d.db database file.
	strippedDBPath := outputDBFile
	strippedDB, err := bolt.Open(strippedDBPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer strippedDB.Close()
        fmt.Println("Created strippeddb")

	// Copy the graph-node bucket to the stripped database.
	if err := copyBucket(existingDB, strippedDB, graphNodeBucketName); err != nil {
		log.Println(err)
	}
	fmt.Println("Copied graph-node bucket")

	// Copy the graph-edge bucket to the stripped database.
	if err := copyBucket(existingDB, strippedDB, graphEdgeBucketName); err != nil {
		log.Println(err)
	}
	fmt.Println("Copied graph-edge bucket")

	fmt.Println("Stripped database created successfully")
	strippedDB.Close()
	// Perform compaction by creating a new database file and copying the contents.
	if err := compactDatabase(strippedDBPath); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Database compaction completed successfully")

	// Generate the MD5 checksum for the graph-001d.db file.
	err = generateMD5Checksum(strippedDBPath, md5SumsFile)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("MD5 checksum generated successfully")
}

func copyBucket(srcDB, destDB *bolt.DB, bucketName string) error {
	return srcDB.View(func(tx *bolt.Tx) error {
		srcBucket := tx.Bucket([]byte(bucketName))
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

			// Skip copying 'zombie-index' bucket inside 'graph-edge' bucket.
			if bucketName == "graph-edge" {
				return srcBucket.ForEach(func(k, v []byte) error {
					if string(k) != "zombie-index" {
						if err := destBucket.Put(k, v); err != nil {
							log.Printf("Failed to put key %s in bucket: %v", string(k), err)
							return err
						}
					}
					return nil
				})
			}

			if err := copyNestedBucket(srcBucket, destBucket); err != nil {
				return err
			}

			return nil
		})
	})
}

func copyNestedBucket(srcBucket, destBucket *bolt.Bucket) error {
	err := srcBucket.ForEach(func(key, value []byte) error {
		if nestedBucket := srcBucket.Bucket(key); nestedBucket != nil {
			nestedDestBucket := destBucket.Bucket(key)
			if nestedDestBucket == nil {
				var err error
				nestedDestBucket, err = destBucket.CreateBucket([]byte(key))
				if err != nil {
					log.Printf("Failed to create nested bucket %s: %v", string(key), err)
					return err
				}
			}

			if err := copyNestedBucket(nestedBucket, nestedDestBucket); err != nil {
				return err
			}
		} else {
			if err := destBucket.Put(key, value); err != nil {
				log.Printf("Failed to put key %s in bucket: %v", string(key), err)
				return err
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

	_, err = fmt.Fprintf(md5sumsFile, "%s  %s\n", checksum, filename)
	if err != nil {
		return err
	}

	return nil
}

func compactDatabase(dbPath string) error {
	// Open the original database file.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create a new compacted database file.
	compactedPath := dbPath + ".compacted"
	compactedDB, err := bolt.Open(compactedPath, 0600, nil)
	if err != nil {
		return err
	}
	defer compactedDB.Close()

	err = db.View(func(tx *bolt.Tx) error {
		// Iterate over all buckets in the source database.
		return tx.ForEach(func(name []byte, srcBucket *bolt.Bucket) error {
			// Create or retrieve the corresponding bucket in the destination database.
			return compactedDB.Update(func(destTx *bolt.Tx) error {
				destBucket, err := destTx.CreateBucketIfNotExists(name)
				if err != nil {
					return err
				}

				// Copy the key-value pairs from the source bucket to the destination bucket.
				if err := copyNestedBucket(srcBucket, destBucket); err != nil {
					return err
				}
				return nil
			})
		})
	})
	if err != nil {
		return err
	}

	// Close the original and compacted databases.
	db.Close()
	compactedDB.Close()

	// Rename the compacted database file to replace the original database file.
	if err := os.Rename(compactedPath, dbPath); err != nil {
		return err
	}

	return nil
}