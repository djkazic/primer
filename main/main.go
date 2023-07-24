package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	bolt "go.etcd.io/bbolt"
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
	//return destFile.Sync()
	return nil
}

func main() {
	log.Println("Primer v0.1.0 starting")
	// initial run
	res := run()
	if !res {
		run()
	}
	ticker := time.NewTicker(time.Hour)
	for {
		select {
		case <-ticker.C:
			res := run()
			if !res {
				run()
			}
		}
	}
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func run() bool {
	log.Println("Cycle start: stopping recoverylnd")
	client, err := client.NewEnvClient()
	if err != nil {
		log.Fatalf("Could not initialize docker client: %w", err)
		return false
	}
	os.Remove(dbFile)
	ctx := context.Background()
	err = client.ContainerStop(ctx, containerName, nil)
	if err != nil {
		log.Printf("Unable to stop container %s: %s", containerName, err)
		return false
	}
	time.Sleep(5 * time.Second)
	log.Println("Copying sourcedb")
	//err = copyFile(originalDBFile, dbFile)
	src, _ := os.Open(originalDBFile)
	dest, _ := os.Create(dbFile)
	_, err = io.Copy(dest, src)
	if err != nil {
		log.Fatal(err)
		return false
	}
	log.Println("Starting recoverylnd")
	ctx = context.Background()
	if err := client.ContainerStart(ctx, containerName, types.ContainerStartOptions{}); err != nil {
		log.Printf("Unable to start container %s: %s", containerName, err)
		return false
	}
	// Open the existing channel.db database file in read-only mode.
	existingDB, err := bolt.Open(dbFile, 0600, &bolt.Options{})
	if err != nil {
		log.Fatal(err)
		return false
	}
	defer existingDB.Close()
	log.Println("Opened channeldb")
	var oldFilePath string
	if fileExists(outputDBFile) {
		fh, err := os.Open(outputDBFile)
		if err != nil {
			log.Fatal(err)
			return false
		}
		defer fh.Close()
		hash := md5.New()
		if _, err := io.Copy(hash, fh); err != nil {
			log.Fatal(err)
			return false
		}
		hashBytes := hash.Sum(nil)[:16]
		hashDir := fmt.Sprintf("/cryptpad/%x", hashBytes)
		os.MkdirAll(hashDir, 0755)
		oldFilePath = hashDir + "/graph-001d.db"
		// Move the old graph-001d.db database file
		log.Printf("Moving old DB to %s", oldFilePath)
		os.Rename(outputDBFile, oldFilePath)
	} else {
		os.Create(outputDBFile)
	}
	// Create a new graph-001d.db database file.
	strippedDBPath := outputDBFile
	strippedDB, err := bolt.Open(strippedDBPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
		return false
	}
	defer strippedDB.Close()
	log.Println("Created strippeddb")
	// Copy the graph-node bucket to the stripped database.
	if err := copyBucket(existingDB, strippedDB, graphNodeBucketName); err != nil {
		log.Println(err)
		return false
	}
	log.Println("Copied graph-node bucket")
	// Copy the graph-edge bucket to the stripped database.
	if err := copyBucket(existingDB, strippedDB, graphEdgeBucketName); err != nil {
		log.Println(err)
		return false
	}
	log.Println("Copied graph-edge bucket")
	log.Println("Stripped database created successfully")
	strippedDB.Close()
	err = os.Chmod(strippedDBPath, 0775)
	// Cleanup
	retainLatestMD5Dirs("/cryptpad", 3)
	matches, err := findMD5Dirs("/cryptpad")
	if err != nil {
		log.Fatalf("Error:", err)
		return false
	}
	for _, match := range matches {
		go func(m string) {
			matchGraph := m + "/graph-001d.db"
			matchPatch := m + "/graph-patch"
			os.Remove(matchPatch)
			cmd := exec.Command("bsdiff", matchGraph, outputDBFile, matchPatch)
			err = cmd.Run()
			if err != nil {
				log.Printf("Failed executing bsdiff: %v", err)
			} else {
				log.Println("bsdiff executed successfully")
			}
		}(match)
	}
	// Generate the MD5 checksum for the graph-001d.db file.
	err = generateMD5Checksum(strippedDBPath, md5SumsFile)
	if err != nil {
		log.Fatal(err)
		return false
	}
	log.Println("MD5 checksum generated successfully")
	return true
}

func retainLatestMD5Dirs(root string, retainCount int) error {
	// md5 hashes are 32 hex characters long
	md5Regex := regexp.MustCompile("^[0-9a-fA-F]{32}$")
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return err
	}
	type dirInfo struct {
		path string
		time time.Time
	}
	var dirs []dirInfo
	for _, file := range files {
		if file.IsDir() && md5Regex.MatchString(file.Name()) {
			fullPath := filepath.Join(root, file.Name())
			dirs = append(dirs, dirInfo{path: fullPath, time: file.ModTime()})
		}
	}
	// Sort directories by time, newest first
	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].time.After(dirs[j].time)
	})
	// If there are more than retainCount directories, delete the oldest ones
	for i := retainCount; i < len(dirs); i++ {
		err = os.RemoveAll(dirs[i].path)
		if err != nil {
			return fmt.Errorf("error deleting directory: %w", err)
		}
	}
	return nil
}

func findMD5Dirs(root string) ([]string, error) {
	// md5 hashes are 32 hex characters long
	md5Regex := regexp.MustCompile("^[0-9a-fA-F]{32}$")
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, file := range files {
		if file.IsDir() && md5Regex.MatchString(file.Name()) {
			matches = append(matches, filepath.Join(root, file.Name()))
		}
	}
	return matches, nil
}

func copyBucket(srcDB, destDB *bolt.DB, bucketName string) error {
	return srcDB.View(func(tx *bolt.Tx) error {
		srcBucket := tx.Bucket([]byte(bucketName))
		log.Printf("Copying %s", bucketName)
		if srcBucket == nil {
			log.Printf("Source bucket %s does not exist. Skipping bucket copy.", bucketName)
			return errors.New("source bucket does not exist")
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

type bucketPair struct {
	src  *bolt.Bucket
	dest *bolt.Bucket
}

func copyNestedBucket(srcBucket, destBucket *bolt.Bucket, bucketName string) error {
	stack := []bucketPair{{src: srcBucket, dest: destBucket}}
	for len(stack) > 0 {
		pair := stack[len(stack)-1]
		stack = stack[:len(stack)-1] // Pop pair from stack
		err := pair.src.ForEach(func(key, value []byte) error {
			if bucketName == graphEdgeBucketName || bucketName == graphNodeBucketName {
				if nestedBucket := pair.src.Bucket(key); nestedBucket != nil {
					strKey := string(key)
					nestedDestBucket := pair.dest.Bucket(key)
					if nestedDestBucket == nil {
						var err error
						nestedDestBucket, err = pair.dest.CreateBucket(key)
						if err != nil {
							log.Printf("Failed to create nested bucket %s: %v", strKey, err)
							return err
						}
						log.Printf("Created nested destBucket %s", strKey)
					}
					if strKey == "zombie-index" || strKey == "chan-index" || strKey == "disabled-edge-policy-index" {
						log.Printf("Skipping copy from %s", strKey)
					} else {
						nestedDestBucket.FillPercent = 1.0
						stack = append(stack, bucketPair{src: nestedBucket, dest: nestedDestBucket}) // Push pair to stack
					}
				} else {
					if (bucketName == graphEdgeBucketName) || (bucketName == graphNodeBucketName) {
						if err := pair.dest.Put(key, value); err != nil {
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
		if err != nil {
			return err
		}
	}
	return nil
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
