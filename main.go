package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/lib/pq"
)

type Config struct {
	S3BucketName string `json:"s3BucketName"`
	BackupDir    string `json:"backupDir"`
	PgUser       string `json:"pgUser"`
	PgPassword   string `json:"pgPassword"`
	PgHost       string `json:"pgHost"`
	PgPort       string `json:"pgPort"`
	AwsRegion    string `json:"awsRegion"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
}

func main() {

	// Configuration
	con := Config{}

	byt, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal("Error reading config file	:", err)
	}
	if json.Unmarshal(byt, &con) != nil {
		log.Fatal("Error unmarshalling config file	:", json.Unmarshal(byt, &con))
	}

	timestamp := time.Now().Format("2006-01-02_15-04-05")

	// Create backup directory if it does not exist
	bkpdir := filepath.Join(con.BackupDir, timestamp)
	os.MkdirAll(bkpdir, os.ModePerm)

	// Set up PostgreSQL connection
	connStr := fmt.Sprintf("user=%s dbname=postgres password=%s host=%s port=%s sslmode=disable", con.PgUser, con.PgPassword, con.PgHost, con.PgPort)

	fmt.Println("connStr:", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error opening SQL connection	:", err)
	}
	defer db.Close()

	// Get list of databases
	rows, err := db.Query("SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		log.Fatal("Error listing databases	:", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {

			log.Fatal("Error scanning databases	:", err)
		}
		databases = append(databases, dbName)
	}
	fmt.Println(databases)

	// Set PGPASSWORD environment variable
	err = os.Setenv("PGPASSWORD", con.PgPassword)
	if err != nil {
		log.Fatal(err)
	}

	// Backup each database
	for _, dbName := range databases {
		log.Printf("Backing up %s\n", dbName)
		backupFile := filepath.Join(bkpdir, fmt.Sprintf("%s_backup.sql", dbName))
		//fmt.Println("Env variables:", os.Environ())
		//fmt.Printf("pg_dump, -U, %s, -F, c, -b, -v, -f, %s, %s", con.PgUser, backupFile, dbName)
		//cmd := exec.Command("pg_dump", "-U", con.PgUser, "-f", backupFile, dbName)
		bkpcmd := fmt.Sprintf("psql -U %s -h %s %s > %s", con.PgUser, con.PgHost, dbName, backupFile)
		cmd := exec.Command("sh", "-c", bkpcmd)
		log.Println("Backupfile:", backupFile)

		// Capture standard output and error
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("Error backing up database: %s\n", err)
			log.Printf("pg_dump output: %s\n", output)
			fmt.Println("Error backing up database:", err)
			fmt.Println("pg_dump output:", string(output))
			return
		}

		log.Printf("Backup of database %s completed successfully\n", dbName)

		// if err := cmd.Run(); err != nil {
		// 	fmt.Println("Error backingup database	:", err)
		// 	log.Fatal(err)
		// }
	}

	// Set up AWS session
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(con.AwsRegion),
		Credentials: credentials.NewStaticCredentials(con.AccessKey, con.SecretKey, ""),
	})
	if err != nil {
		log.Fatal("Error creating S3 session	:", err)
	}

	svc := s3.New(sess)

	// Upload backups to S3
	err = filepath.Walk(filepath.Join(con.BackupDir, timestamp), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal("Error scanning backups	:", err)
		}

		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				log.Fatal("Error in opening directory	:", err)
			}
			defer file.Close()

			_, err = svc.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(con.S3BucketName),
				//Key:    aws.String(filepath.Join(timestamp, info.Name())),
				Key:  aws.String(filepath.ToSlash(filepath.Join("dbbackup", timestamp, info.Name()))),
				Body: file,
			})
			if err != nil {
				log.Fatal("Error uploading objects	:", err)
			}
			log.Printf("Uploaded %s to S3\n", info.Name())
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Backup and upload completed....")

	//Housekeep the backup files
	cmd := exec.Command("sh", "-c", fmt.Sprintf("rm -R %s", bkpdir))
	_, err = cmd.CombinedOutput()
	if err != nil {
		log.Fatal("Error deleting objects	:", err)
	}

	log.Println("*********************END OF LOG************************")
}
