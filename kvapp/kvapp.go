package kvapp

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

type KVApp struct {
	data     map[string]string
	dataLock sync.Mutex
}

func NewKVApp() *KVApp {
	return &KVApp{
		data:     map[string]string{},
		dataLock: sync.Mutex{},
	}
}

func (kv *KVApp) String() string {
	return fmt.Sprintf("%+v", kv.data)
}

func (kv *KVApp) HandleCommand(cmd string) (string, error) {

	fmt.Printf("\n@@@@@@@@@@@@@@@@@@@@@@@Handling command: '%s'\n", cmd)
	op, opArgs, err := splitAtFirstSpace(cmd)
	if err != nil {
		return "", fmt.Errorf("error parsing command: %w", err)
	}
	fmt.Printf("\n@@@@@@@@@@@@@@@@@@@@@ op:'%s' ---- opArgs:'%+v'\n", cmd, opArgs)

	var value string

	switch op {
	case "initial_dummy_command":
		break
	case "set":
		err = kv.handleSet(opArgs)
	case "get":
		value, err = kv.handleGet(opArgs)
	case "delete":
		err = kv.handleDelete(opArgs)
	case "snapshot":
		err = kv.handleSnapshot(opArgs)
	case "restore":
		err = kv.handleRestore(opArgs)
	default:
		err = fmt.Errorf("unknown command: %s", op)
	}
	if err != nil {
		return "", err
	}

	return value, nil
}

func (kv *KVApp) handleSet(keyValue string) error {
	parts := strings.SplitN(string(keyValue), " ", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid set command: %s", keyValue)
	}
	key, value := parts[0], parts[1]
	value = strings.TrimSuffix(value, "\n")

	kv.dataLock.Lock()
	defer kv.dataLock.Unlock()

	kv.data[key] = value
	return nil
}

func (kv *KVApp) handleGet(key string) (string, error) {
	kv.dataLock.Lock()
	defer kv.dataLock.Unlock()

	key = strings.TrimSuffix(key, "\n")
	value, ok := kv.data[key]
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)

	}
	return value, nil
}

func (kv *KVApp) handleDelete(key string) error {
	kv.dataLock.Lock()
	defer kv.dataLock.Unlock()

	key = strings.TrimSuffix(key, "\n")
	_, ok := kv.data[key]
	if !ok {
		return fmt.Errorf("key not found: %s", key)

	}
	delete(kv.data, key)

	return nil
}

func (kv *KVApp) handleSnapshot(filename string) error {
	kv.dataLock.Lock()
	defer kv.dataLock.Unlock()

	json, err := json.Marshal(kv.data)
	if err != nil {
		return err
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(json)
	if err != nil {
		return err
	}

	return nil
}

func (kv *KVApp) handleRestore(filename string) error {
	kv.dataLock.Lock()
	defer kv.dataLock.Unlock()

	snaphotData, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	var data map[string]string
	err = json.Unmarshal(snaphotData, &data)
	if err != nil {
		return err
	}

	return nil
}

func splitAtFirstSpace(msgCommand string) (string, string, error) {
	parts := strings.SplitN(msgCommand, " ", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid command: %s", msgCommand)
	}

	command, value := parts[0], parts[1]

	return command, value, nil
}
