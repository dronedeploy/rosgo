package ros

// IMPORT REQUIRED PACKAGES.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/dronedeploy/rosgo/libgengo"
)

// DEFINE PUBLIC STRUCTURES.

// DynamicMessageType abstracts the schema of a ROS Message whose schema is only known at runtime.  DynamicMessageTypes are created by looking up the relevant schema information from
// ROS Message definition files.  DynamicMessageType implements the rosgo MessageType interface, allowing it to be used throughout rosgo in the same manner as message schemas generated
// at compiletime by gengo.
type DynamicMessageType struct {
	spec         *libgengo.MsgSpec
	nested       map[string]*DynamicMessageType // Map with key string = messageType name.
	jsonPrealloc int
}

// DynamicMessage abstracts an instance of a ROS Message whose type is only known at runtime.  The schema of the message is denoted by the referenced DynamicMessageType, while the
// actual payload of the Message is stored in a map[string]interface{} which maps the name of each field to its value.  DynamicMessage implements the rosgo Message interface, allowing
// it to be used throughout rosgo in the same manner as message types generated at compiletime by gengo.
type DynamicMessage struct {
	dynamicType *DynamicMessageType
	data        map[string]interface{}
}

// DEFINE PRIVATE STRUCTURES.

type dynamicMessageLike interface {
	Serialize(buf *bytes.Buffer) error
	GetDynamicType() *DynamicMessageType
}

// GetDynamicType returns the DynamicMessageType of a DynamicMessage
func (m *DynamicMessage) GetDynamicType() *DynamicMessageType {
	return m.dynamicType
}

// DEFINE PUBLIC GLOBALS.

// DEFINE PRIVATE GLOBALS.

var rosPkgPath string // Colon separated list of paths to search for message definitions on.

var context *libgengo.PkgContext // We'll try to preserve a single message context to avoid reloading each time.

// DEFINE PUBLIC STATIC FUNCTIONS.

// SetRuntimePackagePath sets the ROS package search path which will be used by DynamicMessage to look up ROS message definitions at runtime.
func SetRuntimePackagePath(path string) {
	// We're not going to check that the result is valid, we'll just accept it blindly.
	rosPkgPath = path
	// Reset the message context
	ResetContext()
	// All done.
	return
}

// GetRuntimePackagePath returns the ROS package search path which will be used by DynamicMessage to look up ROS message definitions at runtime.  By default, this will
// be equivalent to the ${ROS_PACKAGE_PATH} environment variable.
func GetRuntimePackagePath() string {
	// If a package path hasn't been set at the time of first use, by default we'll just use the ROS environment default.
	if rosPkgPath == "" {
		rosPkgPath = os.Getenv("ROS_PACKAGE_PATH")
	}
	// All done.
	return rosPkgPath
}

// ResetContext resets the package path context so that a new one will be generated
func ResetContext() {
	context = nil
}

// NewDynamicMessageType generates a DynamicMessageType corresponding to the specified typeName from the available ROS message definitions; typeName should be a fully-qualified
// ROS message type name.  The first time the function is run, a message 'context' is created by searching through the available ROS message definitions, then the ROS message to
// be used for the definition is looked up by name.  On subsequent calls, the ROS message type is looked up directly from the existing context.
func NewDynamicMessageType(typeName string) (*DynamicMessageType, error) {
	t, err := newDynamicMessageTypeNested(typeName, "", nil, nil)
	return t, err
}

// NewDynamicMessageTypeLiteral generates a DynamicMessageType, and returns a copy of the generated type. This is required by DynamicAction.
func NewDynamicMessageTypeLiteral(typeName string) (DynamicMessageType, error) {
	t, err := NewDynamicMessageType(typeName)
	return *t, err
}

// NewDynamicMessageTypeFromSpec creates a DynamicMessageType using a preloaded message specification.
// When loading a service or action, multiple message specs are loaded at once, so `NewDynamicMessageType` is not applicable.
func NewDynamicMessageTypeFromSpec(spec *libgengo.MsgSpec) (*DynamicMessageType, error) {
	if spec == nil {
		return nil, errors.New("spec is empty")
	}
	// Create an empty message type.
	t := &DynamicMessageType{}

	// Create nested map and self-register.
	t.nested = make(map[string]*DynamicMessageType)
	t.nested[spec.FullName] = t

	// Create the nested chain for detecting message recursion and self-register.
	nestedChain := make(map[string]struct{})
	nestedChain[spec.FullName] = struct{}{}

	// Populate the DynamicMessageType data from spec.
	err := t.populateFromSpec(spec, nestedChain)

	return t, err
}

// newDynamicMessageTypeNested generates a DynamicMessageType from the available ROS message definitions.  The first time the function is run, a message 'context' is created by
// searching through the available ROS message definitions, then the ROS message type to use for the defintion is looked up by name.  On subsequent calls, the ROS message type
// is looked up directly from the existing context.  This 'nested' version of the function is able to be called recursively, where packageName should be the typeName of the
// parent ROS message; this is used internally for handling complex ROS messages.
func newDynamicMessageTypeNested(typeName string, packageName string, nested map[string]*DynamicMessageType, nestedChain map[string]struct{}) (*DynamicMessageType, error) {
	// Create an empty message type.
	t := &DynamicMessageType{}

	// If we haven't created a message context yet, better do that.
	if context == nil {
		// Create context for our ROS install.
		c, err := libgengo.NewPkgContext(strings.Split(GetRuntimePackagePath(), ":"))
		if err != nil {
			return t, err
		}
		context = c
	}

	// We need to try to look up the full name, in case we've just been given a short name.
	fullname := typeName

	// The Header type has some special treatment.
	if typeName == "Header" {
		fullname = "std_msgs/Header"
	} else {
		_, ok := context.GetMsgs()[fullname]
		if !ok {
			// Seems like the package_name we were give wasn't the full name.

			// Messages in the same package are allowed to use relative names, so try prefixing the package.
			if packageName != "" {
				fullname = packageName + "/" + fullname
			}
		}
	}

	// Create nested maps if required.
	if nested == nil {
		nested = make(map[string]*DynamicMessageType)
	}
	if nestedChain == nil {
		nestedChain = make(map[string]struct{})
	}

	// The nestedChain is used for recursion detection.
	if _, ok := nestedChain[fullname]; ok {
		// DynamicMessageType already created, recursive messages are scary, return error!
		return nil, errors.New("type already in nested chain, message is recursive")
	}

	// Just return with the messageType if it has been defined already
	if tRegistered, ok := nested[fullname]; ok {
		return tRegistered, nil
	}

	nestedChain[fullname] = struct{}{}

	// Load context for the target message.
	spec, err := context.LoadMsg(fullname)
	if err != nil {
		return t, err
	}

	nested[spec.FullName] = t
	t.nested = nested

	// Unravelling the nested chain, we are done.
	err = t.populateFromSpec(spec, nestedChain)

	// Unravelling the nested chain, we are done.
	delete(nestedChain, fullname)

	// We've successfully made a new message type matching the requested ROS type.
	return t, err
}

// populateFromSpec takes a message spec and fills a DynamicMessageType fields. Expects that we have a valid nested chain map.
func (t *DynamicMessageType) populateFromSpec(spec *libgengo.MsgSpec, nestedChain map[string]struct{}) error {
	// Create nested maps if required.
	if t.nested == nil || nestedChain == nil {
		return errors.New("nested maps were not populated")
	}

	// Now we know all about the message!
	t.spec = spec

	// Just come up with a dumb guess for preallocation. This will get set better on the first call.
	t.jsonPrealloc = 3 + len(spec.Fields)*3

	// Generate the spec for any nested messages.
	for _, field := range spec.Fields {
		if field.IsBuiltin == false {
			_, err := newDynamicMessageTypeNested(field.Type, field.Package, t.nested, nestedChain)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DEFINE PUBLIC RECEIVER FUNCTIONS.

//	DynamicMessageType

// Name returns the full ROS name of the message type; required for ros.MessageType.
func (t *DynamicMessageType) Name() string {
	if t.spec == nil {
		return ""
	}
	return t.spec.FullName
}

// Text returns the full ROS message specification for this message type; required for ros.MessageType.
func (t *DynamicMessageType) Text() string {
	if t.spec == nil {
		return ""
	}
	return t.spec.Text
}

// MD5Sum returns the ROS compatible MD5 sum of the message type; required for ros.MessageType.
func (t *DynamicMessageType) MD5Sum() string {
	if t.spec == nil {
		return ""
	}
	return t.spec.MD5Sum
}

// NewMessage creates a new DynamicMessage instantiating the message type; required for ros.MessageType.
func (t *DynamicMessageType) NewMessage() Message {
	// Don't instantiate messages for incomplete types.
	return t.NewDynamicMessage()
}

// NewDynamicMessage constructs a DynamicMessage with zeroed fields.
func (t *DynamicMessageType) NewDynamicMessage() *DynamicMessage {
	// But otherwise, make a new one.
	d := &DynamicMessage{}
	d.dynamicType = t

	var err error
	d.data, err = t.zeroValueData()
	if err != nil {
		return d
	}
	return d
}

//	DynamicMessage

// Data returns the data map field of the DynamicMessage
func (m *DynamicMessage) Data() map[string]interface{} {
	return m.data
}

// Type returns the ROS type of a dynamic message; required for ros.Message.
func (m *DynamicMessage) Type() MessageType {
	return m.dynamicType
}

// Serialize converts a DynamicMessage into a TCPROS bytestream allowing it to be published to other nodes; required for ros.Message.
func (m *DynamicMessage) Serialize(buf *bytes.Buffer) error {
	if m.dynamicType.spec == nil {
		return errors.New("dynamic message type spec is nil")
	}
	if m.dynamicType.nested == nil {
		return errors.New("dynamic message type nested is nil")
	}

	// THIS METHOD IS BASICALLY AN UNTEMPLATED COPY OF THE TEMPLATE IN LIBGENGO.

	var err error = nil

	// Iterate over each of the fields in the message.
	for _, field := range m.dynamicType.spec.Fields {

		if field.IsArray {
			// It's an array.

			// Look up the item.
			array, ok := m.data[field.Name]
			if !ok {
				return errors.New("Field: " + field.Name + ": No data found.")
			}

			if (reflect.ValueOf(array).Kind() != reflect.Array) && (reflect.ValueOf(array).Kind() != reflect.Slice) {
				return errors.New("Field: " + field.Name + ": expected an array.")
			}

			// If the array is not a fixed length, it begins with a declaration of the array size.
			var size uint32
			if field.ArrayLen < 0 {
				size = uint32(reflect.ValueOf(array).Len())
				if err := binary.Write(buf, binary.LittleEndian, size); err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}
			} else {
				size = uint32(field.ArrayLen)

				// Make sure that the 'fixed length' array that is expected is the correct length. Pad it if necessary.
				reflectLen := uint32(reflect.ValueOf(array).Len())
				if reflectLen < size {
					array, err = padArray(array, field, reflectLen, size)
					if err != nil {
						return errors.Wrap(err, "unable to pad array to correct length")
					}
				}
			}

			// Then we just write out all the elements one after another.
			arrayValue := reflect.ValueOf(array)
			for i := uint32(0); i < size; i++ {
				//Casting the array item to interface type
				var arrayItem interface{} = arrayValue.Index(int(i)).Interface()
				// Need to handle each type appropriately.
				if field.IsBuiltin {
					if field.Type == "string" {
						// Make sure we've actually got a string.
						str, ok := arrayItem.(string)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected string.")
						}
						// The string should start with a declaration of the number of characters.
						var sizeStr uint32 = uint32(len(str))
						if err := binary.Write(buf, binary.LittleEndian, sizeStr); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
						// Then write out the actual characters.
						data := []byte(str)
						if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}

					} else if field.Type == "time" {
						// Make sure we've actually got a time.
						t, ok := arrayItem.(Time)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected ros.Time.")
						}
						// Then write out the structure.
						if err := binary.Write(buf, binary.LittleEndian, t); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}

					} else if field.Type == "duration" {
						// Make sure we've actually got a duration.
						d, ok := arrayItem.(Duration)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected ros.Duration.")
						}
						// Then write out the structure.
						if err := binary.Write(buf, binary.LittleEndian, d); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}

					} else {
						// It's a primitive.

						// Because no runtime expressions in type assertions in Go, we need to manually do this.
						switch field.GoType {
						case "bool":
							// Make sure we've actually got a bool.
							v, ok := arrayItem.(bool)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected bool.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "int8":
							// Make sure we've actually got an int8.
							v, ok := arrayItem.(int8)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected int8.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "int16":
							// Make sure we've actually got an int16.
							v, ok := arrayItem.(int16)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected int16.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "int32":
							// Make sure we've actually got an int32.
							v, ok := arrayItem.(int32)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected int32.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "int64":
							// Make sure we've actually got an int64.
							v, ok := arrayItem.(int64)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected int64.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "uint8":
							// Make sure we've actually got a uint8.
							v, ok := arrayItem.(uint8)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected uint8.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "uint16":
							// Make sure we've actually got a uint16.
							v, ok := arrayItem.(uint16)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected uint16.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "uint32":
							// Make sure we've actually got a uint32.
							v, ok := arrayItem.(uint32)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected uint32.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "uint64":
							// Make sure we've actually got a uint64.
							v, ok := arrayItem.(uint64)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected uint64.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "float32":
							// Make sure we've actually got a float32.
							v, ok := arrayItem.(JsonFloat32)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected JsonFloat32.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v.F); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						case "float64":
							// Make sure we've actually got a float64.
							v, ok := arrayItem.(JsonFloat64)
							if !ok {
								return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected JsonFloat64.")
							}
							// Then write out the value.
							if err := binary.Write(buf, binary.LittleEndian, v.F); err != nil {
								return errors.Wrap(err, "Field: "+field.Name)
							}
						default:
							// Something went wrong.
							return errors.New("we haven't implemented this primitive yet")
						}
					}

				} else {
					// Else it's not a builtin.

					// Confirm the message we're holding is actually the correct type.
					msg, ok := arrayItem.(dynamicMessageLike)
					if !ok {
						return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(arrayItem).Name() + ", expected Message.")
					}
					if msg == nil || msg.GetDynamicType() == nil || msg.GetDynamicType().spec == nil {
						return errors.New("Field: " + field.Name + ": nil pointer to MsgSpec")
					}
					if msg.GetDynamicType().spec.ShortName != field.Type {
						return errors.New("Field: " + field.Name + ": Found msg " + msg.GetDynamicType().spec.ShortName + ", expected " + field.Type + ".")
					}
					// Otherwise, we just recursively serialise it.
					if err = msg.Serialize(buf); err != nil {
						return errors.Wrap(err, "Field: "+field.Name)
					}
				}
			}

		} else {
			// It's a scalar.

			// Look up the item.
			item, ok := m.data[field.Name]
			if !ok {
				return errors.New("Field: " + field.Name + ": No data found.")
			}

			// Need to handle each type appropriately.
			if field.IsBuiltin {
				if field.Type == "string" {
					// Make sure we've actually got a string.
					str, ok := item.(string)
					if !ok {
						return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected string.")
					}
					// The string should start with a declaration of the number of characters.
					var sizeStr uint32 = uint32(len(str))
					if err := binary.Write(buf, binary.LittleEndian, sizeStr); err != nil {
						return errors.Wrap(err, "Field: "+field.Name)
					}
					// Then write out the actual characters.
					data := []byte(str)
					if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
						return errors.Wrap(err, "Field: "+field.Name)
					}

				} else if field.Type == "time" {
					// Make sure we've actually got a time.
					t, ok := item.(Time)
					if !ok {
						return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected ros.Time.")
					}
					// Then write out the structure.
					if err := binary.Write(buf, binary.LittleEndian, t); err != nil {
						return errors.Wrap(err, "Field: "+field.Name)
					}

				} else if field.Type == "duration" {
					// Make sure we've actually got a duration.
					d, ok := item.(Duration)
					if !ok {
						return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected ros.Duration.")
					}
					// Then write out the structure.
					if err := binary.Write(buf, binary.LittleEndian, d); err != nil {
						return errors.Wrap(err, "Field: "+field.Name)
					}

				} else {
					// It's a primitive.

					// Because no runtime expressions in type assertions in Go, we need to manually do this.
					switch field.GoType {
					case "bool":
						// Make sure we've actually got a bool.
						v, ok := item.(bool)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected bool.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "int8":
						// Make sure we've actually got an int8.
						v, ok := item.(int8)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected int8.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "int16":
						// Make sure we've actually got an int16.
						v, ok := item.(int16)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected int16.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "int32":
						// Make sure we've actually got an int32.
						v, ok := item.(int32)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected int32.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "int64":
						// Make sure we've actually got an int64.
						v, ok := item.(int64)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected int64.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "uint8":
						// Make sure we've actually got a uint8.
						v, ok := item.(uint8)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected uint8.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "uint16":
						// Make sure we've actually got a uint16.
						v, ok := item.(uint16)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected uint16.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "uint32":
						// Make sure we've actually got a uint32.
						v, ok := item.(uint32)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected uint32.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "uint64":
						// Make sure we've actually got a uint64.
						v, ok := item.(uint64)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected uint64.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "float32":
						// Make sure we've actually got a float32.
						v, ok := item.(JsonFloat32)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected JsonFloat32.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v.F); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					case "float64":
						// Make sure we've actually got a float64.
						v, ok := item.(JsonFloat64)
						if !ok {
							return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected JsonFloat64.")
						}
						// Then write out the value.
						if err := binary.Write(buf, binary.LittleEndian, v.F); err != nil {
							return errors.Wrap(err, "Field: "+field.Name)
						}
					default:
						// Something went wrong.
						return errors.New("we haven't implemented this primitive yet")
					}
				}

			} else {
				// Else it's not a builtin.

				// Confirm the message we're holding is actually the correct type.
				msg, ok := item.(dynamicMessageLike)
				if !ok {
					return errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(item).Name() + ", expected Message.")
				}
				if msg.GetDynamicType().spec.ShortName != field.Type {
					return errors.New("Field: " + field.Name + ": Found msg " + msg.GetDynamicType().spec.ShortName + ", expected " + field.Type + ".")
				}
				// Otherwise, we just recursively serialise it.
				if err = msg.Serialize(buf); err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}
			}
		}
	}

	// All done.
	return err
}

// Deserialize parses a byte stream into a DynamicMessage, thus reconstructing the fields of a received ROS message; required for ros.Message.
func (m *DynamicMessage) Deserialize(buf *bytes.Reader) error {

	if m.dynamicType.spec == nil {
		return errors.New("dynamic message type spec is nil")
	}
	if m.dynamicType.nested == nil {
		return errors.New("dynamic message type nested is nil")
	}

	// To give more sane results in the event of a decoding issue, we decode into a copy of the data field.
	var err error = nil
	tmpData := make(map[string]interface{})
	m.data = nil

	d := LEByteDecoder{}

	// Iterate over each of the fields in the message.
	for _, field := range m.dynamicType.spec.Fields {

		if field.IsArray {
			// It's an array.

			// The array may be a fixed length, or it may be dynamic.
			size := field.ArrayLen
			if field.ArrayLen < 0 {
				// The array is dynamic, so it starts with a declaration of the number of array elements.
				var usize uint32
				if usize, err = d.DecodeUint32(buf); err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}
				size = int(usize)
			}
			// Create an array of the target type.
			switch field.GoType {
			case "bool":
				tmpData[field.Name], err = d.DecodeBoolArray(buf, size)
			case "int8":
				tmpData[field.Name], err = d.DecodeInt8Array(buf, size)
			case "int16":
				tmpData[field.Name], err = d.DecodeInt16Array(buf, size)
			case "int32":
				tmpData[field.Name], err = d.DecodeInt32Array(buf, size)
			case "int64":
				tmpData[field.Name], err = d.DecodeInt64Array(buf, size)
			case "uint8":
				tmpData[field.Name], err = d.DecodeUint8Array(buf, size)
			case "uint16":
				tmpData[field.Name], err = d.DecodeUint16Array(buf, size)
			case "uint32":
				tmpData[field.Name], err = d.DecodeUint32Array(buf, size)
			case "uint64":
				tmpData[field.Name], err = d.DecodeUint64Array(buf, size)
			case "float32":
				tmpData[field.Name], err = d.DecodeFloat32Array(buf, size)
			case "float64":
				tmpData[field.Name], err = d.DecodeFloat64Array(buf, size)
			case "string":
				tmpData[field.Name], err = d.DecodeStringArray(buf, size)
			case "ros.Time":
				tmpData[field.Name], err = d.DecodeTimeArray(buf, size)
			case "ros.Duration":
				tmpData[field.Name], err = d.DecodeDurationArray(buf, size)
			default:
				if field.IsBuiltin {
					// Something went wrong.
					return errors.New("we haven't implemented this primitive yet")
				}

				// The type encapsulates another ROS message, so we nest a DynamicMessage.
				msgType, err := m.dynamicType.getNestedTypeFromField(&field)
				if err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}

				if tmpData[field.Name], err = d.DecodeMessageArray(buf, size, msgType); err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}
			}
			if err != nil {
				return errors.Wrap(err, "Field: "+field.Name)
			}

		} else { // Else it's a scalar.

			if field.IsBuiltin {
				// It's a regular primitive element.
				switch field.GoType {
				case "bool":
					tmpData[field.Name], err = d.DecodeBool(buf)
				case "int8":
					tmpData[field.Name], err = d.DecodeInt8(buf)
				case "int16":
					tmpData[field.Name], err = d.DecodeInt16(buf)
				case "int32":
					tmpData[field.Name], err = d.DecodeInt32(buf)
				case "int64":
					tmpData[field.Name], err = d.DecodeInt64(buf)
				case "uint8":
					tmpData[field.Name], err = d.DecodeUint8(buf)
				case "uint16":
					tmpData[field.Name], err = d.DecodeUint16(buf)
				case "uint32":
					tmpData[field.Name], err = d.DecodeUint32(buf)
				case "uint64":
					tmpData[field.Name], err = d.DecodeUint64(buf)
				case "float32":
					tmpData[field.Name], err = d.DecodeFloat32(buf)
				case "float64":
					tmpData[field.Name], err = d.DecodeFloat64(buf)
				case "string":
					tmpData[field.Name], err = d.DecodeString(buf)
				case "ros.Time":
					tmpData[field.Name], err = d.DecodeTime(buf)
				case "ros.Duration":
					tmpData[field.Name], err = d.DecodeDuration(buf)
				default:
					// Something went wrong.
					return errors.New("we haven't implemented this primitive yet")
				}

				if err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}

			} else {
				// The type encapsulates another ROS message, so we nest a DynamicMessage.
				msgType, err := m.dynamicType.getNestedTypeFromField(&field)
				if err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}

				if tmpData[field.Name], err = d.DecodeMessage(buf, msgType); err != nil {
					return errors.Wrap(err, "Field: "+field.Name)
				}
			}
		}
	}

	// All done.
	m.data = tmpData
	return err
}

// String returns a string which represents the encapsulared DynamicMessage data.
func (m *DynamicMessage) String() string {
	// Just print out the data!
	return fmt.Sprint(m.dynamicType.Name(), "::", m.data)
}

// DEFINE PRIVATE STATIC FUNCTIONS.

// DEFINE PRIVATE RECEIVER FUNCTIONS.

// zeroValueData creates the zeroValue (default) data map for a new DynamicMessage.
func (t *DynamicMessageType) zeroValueData() (map[string]interface{}, error) {
	//Create map
	d := make(map[string]interface{})

	// Function guards to prevent this call from panicking.
	if t.spec == nil {
		return d, errors.New("dynamic message type spec is nil")
	}
	if t.nested == nil {
		return d, errors.New("dynamic message type nested is nil")
	}

	var err error

	//Range fields in the dynamic message type.
	for _, field := range t.spec.Fields {
		if field.IsArray {
			// If the array length is static set the size, for dynamic arrays, use 0.
			var size int = 0
			if field.ArrayLen > 0 {
				size = field.ArrayLen
			}

			switch field.GoType {
			case "bool":
				d[field.Name] = make([]bool, size)
			case "int8":
				d[field.Name] = make([]int8, size)
			case "int16":
				d[field.Name] = make([]int16, size)
			case "int32":
				d[field.Name] = make([]int32, size)
			case "int64":
				d[field.Name] = make([]int64, size)
			case "uint8":
				d[field.Name] = make([]uint8, size)
			case "uint16":
				d[field.Name] = make([]uint16, size)
			case "uint32":
				d[field.Name] = make([]uint32, size)
			case "uint64":
				d[field.Name] = make([]uint64, size)
			case "float32":
				d[field.Name] = make([]JsonFloat32, size)
			case "float64":
				d[field.Name] = make([]JsonFloat64, size)
			case "string":
				d[field.Name] = make([]string, size)
			case "ros.Time":
				d[field.Name] = make([]Time, size)
			case "ros.Duration":
				d[field.Name] = make([]Duration, size)
			default:
				if field.IsBuiltin {
					// Something went wrong.
					return d, errors.New("we haven't implemented this primitive yet")
				}

				// The type encapsulates another ROS message, so we nest a DynamicMessage.
				msgType, err := t.getNestedTypeFromField(&field)
				if err != nil {
					return d, errors.Wrap(err, "Field: "+field.Name)
				}
				// Fill out our new messages.
				messages := make([]Message, size)
				for i := 0; i < size; i++ {
					messages[i] = msgType.NewMessage()
				}
				d[field.Name] = messages
			}
		} else { // Not an array.
			if field.IsBuiltin {
				// If it is a built in type.
				switch field.GoType {
				case "string":
					d[field.Name] = ""
				case "bool":
					d[field.Name] = bool(false)
				case "int8":
					d[field.Name] = int8(0)
				case "int16":
					d[field.Name] = int16(0)
				case "int32":
					d[field.Name] = int32(0)
				case "int64":
					d[field.Name] = int64(0)
				case "uint8":
					d[field.Name] = uint8(0)
				case "uint16":
					d[field.Name] = uint16(0)
				case "uint32":
					d[field.Name] = uint32(0)
				case "uint64":
					d[field.Name] = uint64(0)
				case "float32":
					d[field.Name] = JsonFloat32{F: float32(0.0)}
				case "float64":
					d[field.Name] = JsonFloat64{F: float64(0.0)}
				case "ros.Time":
					d[field.Name] = Time{}
				case "ros.Duration":
					d[field.Name] = Duration{}
				default:
					return d, errors.Wrap(err, "builtin field "+field.GoType+" not found")
				}
			} else {
				// The type encapsulates another ROS message, so we nest a DynamicMessage.
				msgType, err := t.getNestedTypeFromField(&field)
				if err != nil {
					return d, errors.Wrap(err, "Field: "+field.Name)
				}
				d[field.Name] = msgType.NewMessage()
			}
		}
	}
	return d, err
}

// Get a nested type of a dynamic message from a field.
func (t *DynamicMessageType) getNestedTypeFromField(field *libgengo.Field) (*DynamicMessageType, error) {
	if t.nested == nil {
		return nil, errors.New("cannot get nested type from invalid dynamic message type")
	}
	fieldtype := field.Package + "/" + field.Type

	if msgType, ok := t.nested[fieldtype]; ok {
		return msgType, nil
	}
	// Did not find the message type. Return an error.
	return nil, errors.Wrap(errors.New("nested map does not contain requested field"), "fieldtype: "+fieldtype)
}

// padArray pads the provided array to the specified length using the default value for the array type.
func padArray(array interface{}, field libgengo.Field, actualSize, requiredSize uint32) (interface{}, error) {
	switch field.GoType {
	case "bool":
		// Make sure we've actually got a bool.
		v, ok := array.([]bool)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []bool.")
		}
		padding := make([]bool, requiredSize-actualSize)
		for i := range padding {
			padding[i] = false
		}
		v = append(v, padding...)
		return v, nil
	case "int8":
		// Make sure we've actually got an int8.
		v, ok := array.([]int8)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []int8.")
		}
		padding := make([]int8, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "int16":
		// Make sure we've actually got an int16.
		v, ok := array.([]int16)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []int16.")
		}
		padding := make([]int16, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "int32":
		// Make sure we've actually got an int32.
		v, ok := array.([]int32)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []int32.")
		}
		padding := make([]int32, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "int64":
		// Make sure we've actually got an int64.
		v, ok := array.([]int64)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []int64.")
		}
		padding := make([]int64, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "uint8":
		// Make sure we've actually got a uint8.
		v, ok := array.([]uint8)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []uint8.")
		}
		padding := make([]uint8, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "uint16":
		// Make sure we've actually got a uint16.
		v, ok := array.([]uint16)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []uint16.")
		}
		padding := make([]uint16, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "uint32":
		// Make sure we've actually got a uint32.
		v, ok := array.([]uint32)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []uint32.")
		}
		padding := make([]uint32, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "uint64":
		// Make sure we've actually got a uint64.
		v, ok := array.([]uint64)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []uint64.")
		}
		padding := make([]uint64, requiredSize-actualSize)
		for i := range padding {
			padding[i] = 0
		}
		v = append(v, padding...)
		return v, nil
	case "float32":
		// Make sure we've actually got a float32.
		v, ok := array.([]JsonFloat32)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []JsonFloat32.")
		}
		padding := make([]JsonFloat32, requiredSize-actualSize)
		for i := range padding {
			padding[i] = JsonFloat32{F: 0}
		}
		v = append(v, padding...)
		return v, nil
	case "float64":
		// Make sure we've actually got a float64.
		v, ok := array.([]JsonFloat64)
		if !ok {
			return nil, errors.New("Field: " + field.Name + ": Found " + reflect.TypeOf(array).Name() + ", expected []JsonFloat64.")
		}
		padding := make([]JsonFloat64, requiredSize-actualSize)
		for i := range padding {
			padding[i] = JsonFloat64{F: 0}
		}
		v = append(v, padding...)
		return v, nil
	default:
		// Something went wrong.
		return nil, errors.New("we haven't implemented this primitive yet")
	}
}

// ALL DONE.
