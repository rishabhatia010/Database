package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jcelliott/lumber"
)

const version = "0.0.1"

// Database struct to store users with read/write locking.
type Database struct {
	sync.RWMutex
	users map[string]User
}

// Driver struct to manage the file-based database and logging.
type Driver struct {
	mutex   sync.Mutex
	mutexes map[string]*sync.Mutex
	dir     string
	log     Logger
}

// Options struct to hold optional configurations like Logger.
type Options struct {
	Logger
}

// User struct representing user data
type User struct {
	Name    string
	Age     json.Number
	Company string
	Address string
}

// Address struct nested within User
type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

// Logger interface for various logging levels.
type Logger interface {
	Fatal(string, ...interface{})
	Error(string, ...interface{})
	Info(string, ...interface{})
	Debug(string, ...interface{})
}

// New initializes a new database driver.
func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}
	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := &Driver{
		dir:     dir,
		log:     opts.Logger,
		mutexes: make(map[string]*sync.Mutex),
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		opts.Logger.Info("Creating database directory at '%s'", dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("could not create database directory: %v", err)
		}
	} else {
		opts.Logger.Debug("Using existing database directory '%s'", dir)
	}

	return driver, nil
}

// Write saves a User object to the specified directory and file.
func (d *Driver) Write(collection, key string, value User) error {
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("could not create collection directory: %v", err)
	}

	filePath := filepath.Join(dir, key+".json")
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("could not create file: %v", err)
	}
	defer file.Close()

	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal data: %v", err)
	}

	if _, err = file.Write(data); err != nil {
		return fmt.Errorf("could not write data to file: %v", err)
	}

	d.log.Info("Wrote user %s to collection %s", key, collection)
	return nil
}

// Read retrieves a single User object by key.
func (d *Driver) Read(collection, key string) (User, error) {
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	filePath := filepath.Join(d.dir, collection, key+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return User{}, fmt.Errorf("could not read file: %v", err)
	}

	var user User
	if err = json.Unmarshal(data, &user); err != nil {
		return User{}, fmt.Errorf("could not unmarshal data: %v", err)
	}

	return user, nil
}

// ReadAll retrieves all User objects in a collection.
func (d *Driver) ReadAll(collection string) ([]User, error) {
	dir := filepath.Join(d.dir, collection)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("could not read directory: %v", err)
	}

	var users []User
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".json") {
			// Extracting the user key by trimming the .json suffix
			key := strings.TrimSuffix(file.Name(), ".json")
			user, err := d.Read(collection, key)
			if err != nil {
				d.log.Error("Error reading user file %s: %v", file.Name(), err)
				continue
			}
			users = append(users, user)
		}
	}
	return users, nil
}

// Delete removes a specific User object by key.
func (d *Driver) Delete(collection, key string) error {
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	filePath := filepath.Join(d.dir, collection, key+".json")
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("could not delete file: %v", err)
	}

	d.log.Info("Deleted user %s from collection %s", key, collection)
	return nil
}

// getOrCreateMutex provides a mutex for a specific collection.
func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.mutexes == nil {
		d.mutexes = make(map[string]*sync.Mutex)
	}

	mutex, exists := d.mutexes[collection]
	if !exists {
		mutex = &sync.Mutex{}
		d.mutexes[collection] = mutex
	}

	return mutex
}

func main() {
	dir := "./db"

	// Initialize the database
	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error initializing database:", err)
		os.Exit(1)
	}

	// Interactive menu for reading and deleting
	for {
		fmt.Println("\nChoose an operation:")
		fmt.Println("1. Add a new user")
		fmt.Println("2. Read a user by name")
		fmt.Println("3. Read all users")
		fmt.Println("4. Delete a user by name")
		fmt.Println("5. Exit")
		var choice int
		fmt.Print("Enter your choice: ")
		fmt.Scanln(&choice)

		switch choice {
		case 1:
			// Add a new user
			var name, age, company, address string
			fmt.Print("Name: ")
			fmt.Scanln(&name)
			fmt.Print("Age: ")
			fmt.Scanln(&age)
			fmt.Print("Company: ")
			fmt.Scanln(&company)
			fmt.Print("Address: ")
			fmt.Scanln(&address)

			user := User{Name: name, Age: json.Number(age), Company: company, Address: address}
			if err := db.Write("users", name, user); err != nil {
				fmt.Println("Error writing user:", err)
			} else {
				fmt.Printf("User %s added successfully.\n", name)
			}

		case 2:
			// Read a specific user by name
			var userName string
			fmt.Print("Enter user name to read: ")
			fmt.Scanln(&userName)
			user, err := db.Read("users", userName)
			if err != nil {
				fmt.Printf("Error reading user %s: %v\n", userName, err)
			} else {
				fmt.Printf("Retrieved user %s: %+v\n", userName, user)
			}

		case 3:
			// Read all users
			allUsers, err := db.ReadAll("users")
			if err != nil {
				fmt.Println("Error reading all users:", err)
			} else {
				fmt.Println("All users retrieved:")
				for _, u := range allUsers {
					fmt.Printf("%+v\n", u)
				}
			}

		case 4:
			// Delete a specific user by name
			var userName string
			fmt.Print("Enter user name to delete: ")
			fmt.Scanln(&userName)
			if err := db.Delete("users", userName); err != nil {
				fmt.Printf("Error deleting user %s: %v\n", userName, err)
			} else {
				fmt.Printf("User %s deleted successfully.\n", userName)
			}

		case 5:
			// Exit the program
			fmt.Println("Exiting program.")
			return

		default:
			fmt.Println("Invalid choice, please try again.")
		}
	}
}
