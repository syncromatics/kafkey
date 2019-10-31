package service

import (
	"encoding/binary"
	"fmt"
	"time"

	schemaregistry "github.com/Landoop/schema-registry"
	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
)

// Registry is a kafka registry client
type Registry struct {
	client *schemaregistry.Client
	codecs map[int32]*goavro.Codec
}

// NewRegistry creates a new registry
func NewRegistry(url string) (*Registry, error) {
	r, err := schemaregistry.NewClient(url)
	if err != nil {
		return nil, err
	}

	return &Registry{r, map[int32]*goavro.Codec{}}, nil
}

// RegisterNewSchema registers a new schema for the topic
func (r *Registry) RegisterNewSchema(subject string, schema string) (int, error) {
	return r.client.RegisterNewSchema(subject, schema)
}

// GetSchemaByID returns the schema for the id
func (r *Registry) GetSchemaByID(subjectID int) (string, error) {
	return r.client.GetSchemaByID(subjectID)
}

// WaitForRegistryToBeReady will wait til it can contact the registry
func (r *Registry) WaitForRegistryToBeReady(timeout time.Duration) error {
	var err error
	now := time.Now()
	for time.Now().Before(now.Add(timeout)) {
		_, err = r.client.Subjects()
		if err != nil {
			continue
		}

		return nil
	}

	if err == nil {
		return fmt.Errorf("timeout waiting for registry to be ready")
	}

	return errors.Wrap(err, "timeout waiting for registry to be ready")
}

func (r *Registry) Decode(message []byte) (string, error) {
	if err := validateAvro(message); err != nil {
		return "", err
	}

	id := int32(binary.BigEndian.Uint32(message[1:5]))

	c, ok := r.codecs[id]
	if !ok {
		s, err := r.GetSchemaByID(int(id))
		if err != nil {
			return "", errors.Wrap(err, "failed to get schema")
		}

		c, err = goavro.NewCodec(s)
		if err != nil {
			return "", errors.Wrap(err, "failed to get schema")
		}
		r.codecs[id] = c
	}

	native, _, err := c.NativeFromBinary(message[5:])
	if err != nil {
		return "", errors.Wrap(err, "failed to NativeFromBinary")
	}

	b, err := c.TextualFromNative(nil, native)
	if err != nil {
		return "", errors.Wrap(err, "failed to TextualFromNative")
	}

	return string(b), nil
}

func validateAvro(b []byte) error {
	if len(b) == 0 {
		return errors.New("avro: payload is empty")
	}

	if b[0] != 0 {
		return errors.Errorf("avro: wrong magic byte for confluent avro encoding: %v", b[0])
	}

	// Message encoded with Confluent Avro encoding cannot be less than 5 bytes,
	// because first byte is a magic byte, and next 4 bytes is a mandatory schema ID.
	if len(b) < 5 {
		return errors.New("avro: payload is less than 5 bytes")
	}

	return nil
}
