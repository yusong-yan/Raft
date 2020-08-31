package myrpc

import (
	"bytes"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"../mygob"
)

type reqMsg struct {
	endname  interface{}
	svcMeth  string
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok     bool
	replyB []byte
}

type ClientEnd struct {
	endname interface{}
	ch      chan reqMsg //this just the copy of whole network's reqMsg
	done    chan struct{}
}

func (client *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	currentReqMsg := reqMsg{}
	currentReqMsg.endname = client.endname
	currentReqMsg.svcMeth = svcMeth
	currentReqMsg.argsType = reflect.TypeOf(args)
	currentReqMsg.replyCh = make(chan replyMsg)

	bf := new(bytes.Buffer)
	enC := mygob.NewEncoder(bf)
	enC.Encode(args)
	currentReqMsg.args = bf.Bytes()

	select {
	case client.ch <- currentReqMsg:
	case <-client.done:
		return false
	}

	currentRepMsg := <-currentReqMsg.replyCh
	if currentRepMsg.ok {
		rb := bytes.NewBuffer(currentRepMsg.replyB)
		rd := mygob.NewDecoder(rb)
		err := rd.Decode(reply)
		if err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int // incoming RPCs
}

type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method
}

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool                        // pause a long time on send on disabled connection
	longReordering bool                        // sometimes delay replies a long time
	ends           map[interface{}]*ClientEnd  // ends, by name
	enabled        map[interface{}]bool        // by end name
	servers        map[interface{}]*Server     // servers, by name
	connections    map[interface{}]interface{} // endname -> servername
	endCh          chan reqMsg
	done           chan struct{} // closed when Network is cleaned up
	count          int32         // total RPC count, for statistics
	bytes          int64         // total bytes send, for statistics
}

func MakeNetwork() *Network {
	nw := &Network{}
	nw.reliable = true
	nw.ends = map[interface{}]*ClientEnd{}
	nw.enabled = map[interface{}]bool{}
	nw.servers = map[interface{}]*Server{}
	nw.connections = map[interface{}](interface{}){}
	nw.endCh = make(chan reqMsg)
	nw.done = make(chan struct{})

	go func() {
		for {
			select {
			case req := <-nw.endCh:
				atomic.AddInt32(&nw.count, 1)
				atomic.AddInt64(&nw.bytes, int64(len(req.args)))
				go nw.processReq(req)
			case <-nw.done:
				return
			}
		}
	}()

	return nw
}
func (nw *Network) processReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := nw.readEndnameInfo(req.endname)
	if servername != nil && server != nil && enabled {
		if reliable == false {
			time.Sleep(time.Duration((rand.Int() % 27)) * time.Millisecond)
		}
		if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
			return
		}

		successProcessed := make(chan replyMsg)
		go func() {
			successProcessed <- server.dispatch(req)
		}()

		var reply replyMsg
		getReply := false
		serverIsDead := false
		for getReply == false && serverIsDead == false {
			select {
			case reply = <-successProcessed:
				getReply = true
			case <-time.After(100 * time.Millisecond):
				serverIsDead = nw.isServerDead(req.endname, servername, server)
				if serverIsDead {
					go func() {
						<-successProcessed
					}()
				}
			}
		}
		serverIsDead = nw.isServerDead(req.endname, servername, server)

		if getReply == false || serverIsDead == true {
			req.replyCh <- replyMsg{false, nil}
		} else if reliable == false && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering == true && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				atomic.AddInt64(&nw.bytes, int64(len(reply.replyB)))
				req.replyCh <- reply
			})
		} else {
			atomic.AddInt64(&nw.bytes, int64(len(reply.replyB)))
			req.replyCh <- reply
		}
	} else {
		ms := 0
		if nw.longDelays {
			ms = (rand.Int() % 7000)
		} else {
			ms = (rand.Int() % 100)
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
	}
}
func (nw *Network) readEndnameInfo(endname interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longreordering bool,
) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	enabled = nw.enabled[endname]
	servername = nw.connections[endname]
	if servername != nil {
		server = nw.servers[servername]
	}
	reliable = nw.reliable
	longreordering = nw.longReordering
	return
}
func (rn *Network) Cleanup() {
	close(rn.done)
}
func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}
func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}
func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

func (nw *Network) MakeEnd(endname interface{}) *ClientEnd {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	if _, ok := nw.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	newClientEnd := &ClientEnd{}
	newClientEnd.done = nw.done
	newClientEnd.ch = nw.endCh
	newClientEnd.endname = endname
	nw.ends[endname] = newClientEnd
	nw.connections[endname] = nil
	nw.enabled[endname] = false

	return newClientEnd
}
func (nw *Network) Connect(endname interface{}, servername interface{}) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.connections[endname] = servername
}
func (nw *Network) Enable(endname interface{}, enabled bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.enabled[endname] = enabled
}
func (nw *Network) GetCount(servername interface{}) int {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	svr := nw.servers[servername]
	return svr.GetCount()
}
func (nw *Network) GetTotalCount() int {
	x := atomic.LoadInt32(&nw.count)
	return int(x)
}
func (nw *Network) GetTotalBytes() int64 {
	x := atomic.LoadInt64(&nw.bytes)
	return x
}

func MakeServer() *Server {
	ser := &Server{}
	ser.services = map[string]*Service{}
	return ser
}
func (nw *Network) isServerDead(endname interface{}, servername interface{}, server *Server) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	if nw.enabled[endname] == false || nw.servers[servername] != server {
		return true
	}
	return false
}
func (ser *Server) GetCount() int {
	ser.mu.Lock()
	defer ser.mu.Unlock()
	return ser.count
}
func (nw *Network) AddServer(servername interface{}, ser *Server) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.servers[servername] = ser
}
func (nw *Network) DeleteServer(servername interface{}) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.servers[servername] = nil
}
func (ser *Server) dispatch(req reqMsg) replyMsg {
	ser.mu.Lock()
	ser.count += 1
	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]
	service, ok := ser.services[serviceName]

	ser.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k, _ := range ser.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name
		if method.PkgPath != "" || // capitalized?
			mtype.NumIn() != 3 ||
			mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 0 {
		} else {
			svc.methods[mname] = method
		}
	}
	return svc
}
func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}
func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {

		args := reflect.New(req.argsType)

		// decode the argument.
		ab := bytes.NewBuffer(req.args)
		ad := mygob.NewDecoder(ab)
		ad.Decode(args.Interface())

		// allocate space for the reply.
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		// call the method.
		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		// encode the reply.
		rb := new(bytes.Buffer)
		re := mygob.NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}
