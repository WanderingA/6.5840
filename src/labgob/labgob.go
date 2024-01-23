package labgob

// 尝试通过 RPC 发送非大写字段会产生一系列错误行为，
// 包括神秘的错误计算和直接崩溃。


import "encoding/gob"
import "io"
import "reflect"								// 用于元编程，允许程序在运行时检查类型、变量和方法
import "fmt"
import "sync"
import "unicode"
import "unicode/utf8"

var mu sync.Mutex										
var errorCount int 								// for TestCapital
var checked map[reflect.Type]bool				// Key:reflect.Type, Value:bool，用于检查类型是否已经检查过

type LabEncoder struct {
	gob *gob.Encoder							// gob 编码器
}
// 创建一个新的编码器
func NewEncoder(w io.Writer) *LabEncoder {		
	enc := &LabEncoder{}						
	enc.gob = gob.NewEncoder(w)					
	return enc	
}
// 编码器的 Encode 方法
func (enc *LabEncoder) Encode(e interface{}) error {
	checkValue(e)
	return enc.gob.Encode(e)
}
// 编码器的 EncodeValue 方法
func (enc *LabEncoder) EncodeValue(value reflect.Value) error {
	checkValue(value.Interface())			//value.Interface() 返回 value 的值
	return enc.gob.EncodeValue(value)
}
// 解码器
type LabDecoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *LabDecoder {
	dec := &LabDecoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}

func (dec *LabDecoder) Decode(e interface{}) error {
	checkValue(e)
	checkDefault(e)
	return dec.gob.Decode(e)
}

func Register(value interface{}) {
	checkValue(value)
	gob.Register(value)
}

func RegisterName(name string, value interface{}) {
	checkValue(value)
	gob.RegisterName(name, value)
}
// 检查值的类型
func checkValue(value interface{}) {
	checkType(reflect.TypeOf(value))
}
// 检查类型
func checkType(t reflect.Type) {
	k := t.Kind()									// 获取类型的种类

	mu.Lock()										// 互斥锁
	// 只编译一次，避免重复编译 
	if checked == nil {								// 如果 checked 为空
		checked = map[reflect.Type]bool{}			
	}
	if checked[t] {									// 如果检查过了
		mu.Unlock()
		return
	}
	checked[t] = true								// 标记为已检查
	mu.Unlock()

	switch k {
	case reflect.Struct:							// 如果是结构体	
		for i := 0; i < t.NumField(); i++ {			// 遍历结构体的每个字段
			f := t.Field(i)							// i字段
			rune, _ := utf8.DecodeRuneInString(f.Name)	// 获取字段名的第一个字符
			if unicode.IsUpper(rune) == false {			// 如果不是大写
				// ta da 
				fmt.Printf("labgob error: lower-case field %v of %v in RPC or persist/snapshot will break your Raft\n",
					f.Name, t.Name())
				mu.Lock()
				errorCount += 1						// 错误计数
				mu.Unlock()
			}
			checkType(f.Type)						// 检查字段的类型
		}
		return
	case reflect.Slice, reflect.Array, reflect.Ptr:	// 如果是切片、数组、指针
		checkType(t.Elem())							// 检查元素的类型
		return
	case reflect.Map:								// 如果是Map
		checkType(t.Elem())
		checkType(t.Key())
		return
	default:
		return
	}
}

//
// 如果值包含非默认值，GOB 将发出警告、
// 如果 RPC 回复包含默认值，GOB 不会覆盖非默认值。
//
func checkDefault(value interface{}) {
	if value == nil {
		return
	}
	checkDefault1(reflect.ValueOf(value), 1, "")
}

func checkDefault1(value reflect.Value, depth int, name string) {
	if depth > 3 {
		return
	}

	t := value.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			vv := value.Field(i)
			name1 := t.Field(i).Name
			if name != "" {
				name1 = name + "." + name1
			}
			checkDefault1(vv, depth+1, name1)
		}
		return
	case reflect.Ptr:
		if value.IsNil() {
			return
		}
		checkDefault1(value.Elem(), depth+1, name)
		return
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.String:
		if reflect.DeepEqual(reflect.Zero(t).Interface(), value.Interface()) == false {
			mu.Lock()
			if errorCount < 1 {
				what := name
				if what == "" {
					what = t.Name()
				}
				// this warning typically arises if code re-uses the same RPC reply
				// variable for multiple RPC calls, or if code restores persisted
				// state into variable that already have non-default values.
				fmt.Printf("labgob warning: Decoding into a non-default variable/field %v may not work\n",
					what)
			}
			errorCount += 1
			mu.Unlock()
		}
		return
	}
}
