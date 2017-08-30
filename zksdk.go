package zksdk

import (
	"errors"
	"fmt"
	logger "github.com/alecthomas/log4go"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const (
	FlagNormal    = 0
	FlagEphemeral = 1
	FlagSequence  = 2
)

const (
	Default_StartTimeout = 30
	Default_PubChSize    = 128
	Default_ConErrSleep  = 2
)

type TPubMsg struct {
	Path    string
	Data    []byte
	Sons    []string
	DelFlag bool
	Ver     int32
	CVer    int32
	Stat    *zk.Stat
}

type SonHandler interface {
	Proc(all, add, del []string, stat *zk.Stat, delFlag bool)
}

type DataHandler interface {
	Proc(data []byte, stat *zk.Stat, delFlag bool)
}

var DefaultSdk ZkSdk

type ZkSdk struct {
	mConn        *zk.Conn
	mEventCh     <-chan zk.Event
	mServers     []string
	mSessionTime time.Duration

	mTmpNodes     map[string][]byte //会话上就必须要创建的临时节点
	mDelNodes     map[string]int32  //会话连上就必须要删除的节点
	mTmpNodesLock sync.Mutex
	mDelNodesLock sync.Mutex

	mOneOkCh   chan bool
	mExitCh    chan bool      //退出ch
	mRunFlag   bool           //运行标志
	mWaitGroup sync.WaitGroup //go程同步
}

func (this *ZkSdk) Init(servers []string, timeout time.Duration) error {
	if servers == nil {
		return fmt.Errorf("nil servers")
	}
	this.mServers = servers
	this.mTmpNodes = make(map[string][]byte, 0)
	this.mDelNodes = make(map[string]int32, 0)
	this.mExitCh = make(chan bool, 1)
	this.mOneOkCh = make(chan bool, 1)
	this.mSessionTime = timeout
	return nil
}

//保持临时节点永活
func (this *ZkSdk) AddTmpAlive(path string, data []byte) {
	this.mTmpNodesLock.Lock()
	defer this.mTmpNodesLock.Unlock()
	this.mTmpNodes[path] = data
}

//去除永活节点
func (this *ZkSdk) DelTmpAlive(path string) {
	this.mTmpNodesLock.Lock()
	defer this.mTmpNodesLock.Unlock()
	delete(this.mTmpNodes, path)
}

//添加必须删除节点
func (this *ZkSdk) AddDeadNode(path string, ver int32) {
	this.mDelNodesLock.Lock()
	defer this.mDelNodesLock.Unlock()
	this.mDelNodes[path] = ver
}

//移除必须删除节点
func (this *ZkSdk) DelDeadNode(path string) {
	this.mDelNodesLock.Lock()
	defer this.mDelNodesLock.Unlock()
	delete(this.mDelNodes, path)
}

func (this *ZkSdk) StartX() (ech <-chan zk.Event, err error) {
	this.mConn, ech, err = zk.Connect(this.mServers, this.mSessionTime)
	if err != nil {
		return nil, err
	}

	logger.Info("ZkSdk start[%v] ok", this.mServers)
	this.mRunFlag = true
	return ech, err
}

//至少连上一次才算启动成功
func (this *ZkSdk) Start(oneOk ...bool) (err error) {
	this.mConn, this.mEventCh, err = zk.Connect(this.mServers, this.mSessionTime)
	if err != nil {
		return err
	}
	go this.Monitor()

	if len(oneOk) != 0 {
		select {
		case <-this.mOneOkCh:
		case <-time.After(time.Second * Default_StartTimeout):
			this.mConn.Close()
			this.mConn = nil
			return errors.New("Zk nohave StateHasSession in 30s")
		}
	}
	logger.Info("ZkSdk start[%v] ok", this.mServers)
	this.mRunFlag = true
	return err
}

func (this *ZkSdk) Close() {
	this.mRunFlag = false
	if this.mExitCh != nil {
		close(this.mExitCh)
		this.mExitCh = nil
	}
	this.mWaitGroup.Wait()
	if this.mConn != nil {
		this.mConn.Close()
		this.mConn = nil
	}
	logger.Info("ZkSdk Close ok")
}

func (this *ZkSdk) CreateNode(path string, data []byte, flag int32) (string, error) {
	if !this.mRunFlag {
		return "", fmt.Errorf("ZkSdk not started")
	}
	if len(path) == 0 {
		return "", errors.New("nil path")
	}
	p, err := this.mConn.Create(path, data, this.toFlag(flag), zk.WorldACL(zk.PermAll))
	return p, err
}

//父节点不存在就创建
func (this *ZkSdk) CreateDepthNode(path string, data []byte, flag int32) (string, error) {
	nodeArr := strings.Split(path, "/")
	addPath := ""
	for i := 0; i < len(nodeArr)-1; i++ {
		if len(nodeArr[i]) == 0 {
			continue
		}
		addPath += ("/" + nodeArr[i])
		exist, _, err := this.mConn.Exists(addPath)
		if err != nil {
			return "", err
		}
		if exist {
			continue
		}
		if _, cerr := this.mConn.Create(addPath, []byte(""), FlagNormal, zk.WorldACL(zk.PermAll)); cerr != nil {
			return "", cerr
		}
	}
	return this.mConn.Create(path, data, flag, zk.WorldACL(zk.PermAll))
}

func (this *ZkSdk) DeleteNode(path string, version int32) error {
	if !this.mRunFlag {
		return fmt.Errorf("ZkSdk not started")
	}
	if len(path) == 0 {
		return errors.New("nil path")
	}
	return this.mConn.Delete(path, version)
}

func (this *ZkSdk) Exists(path string) (bool, *zk.Stat, error) {
	if !this.mRunFlag {
		return false, nil, fmt.Errorf("ZkSdk not started")
	}
	exists, stat, err := this.mConn.Exists(path)
	return exists, stat, err
}

func (this *ZkSdk) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	if !this.mRunFlag {
		return false, nil, nil, fmt.Errorf("ZkSdk not started")
	}
	return this.mConn.ExistsW(path)
}

func (this *ZkSdk) GetData(path string) ([]byte, *zk.Stat, error) {
	if !this.mRunFlag {
		return nil, nil, fmt.Errorf("ZkSdk not started")
	}
	data, state, err := this.mConn.Get(path)
	return data, state, err
}

func (this *ZkSdk) SetData(path string, data []byte, ver int32) (*zk.Stat, error) {
	if !this.mRunFlag {
		return nil, fmt.Errorf("ZkSdk not started")
	}
	if len(path) == 0 {
		return nil, errors.New("nil path")
	}
	stat, err := this.mConn.Set(path, data, ver)
	return stat, err
}

//获取,并订阅一次变化通知
func (this *ZkSdk) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if !this.mRunFlag {
		return nil, nil, nil, fmt.Errorf("ZkSdk not started")
	}
	return this.mConn.GetW(path)
}

//获取,并持续订阅数据变化
func (this *ZkSdk) GetMoreW(path string, exitCh chan bool) ([]byte, *zk.Stat, <-chan *TPubMsg, error) {
	if !this.mRunFlag {
		return nil, nil, nil, fmt.Errorf("ZkSdk not started")
	}
	data, stat, oneEventCh, err := this.mConn.GetW(path)
	if err != nil {
		return nil, stat, nil, err
	}
	watchPubCh := make(chan *TPubMsg, Default_PubChSize)
	oldVer := stat.Version
	go func() {
		defer close(watchPubCh)
		var event zk.Event
		event.Type = zk.EventNotWatching
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		logger.Info("ZkSdk GetMoreW[%s]", path)
		defer logger.Info("ZkSdk UnSubGetMoreW[%s]", path)
		for {
			select {
			case event = <-oneEventCh:
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}

			if event.Type == zk.EventNodeDeleted {
				logger.Info("node[%s] Deleted", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}

		HEREGetMoreW:
			data, stat, oneEventCh, err = this.mConn.GetW(path)
			if err == zk.ErrNoNode {
				logger.Info("node[%s] noExist", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}
			if err != nil {
				logger.Error("Getw[%s] err[%v] sleep[%d] and continue", path, err, Default_ConErrSleep)
				time.Sleep(time.Second * Default_ConErrSleep)
				goto HEREGetMoreW
			}
			if stat.Version <= oldVer {
				continue
			}
			oldVer = stat.Version
			logger.Debug("node[%s] data change[%s]", path, string(data))
			watchPubCh <- &TPubMsg{
				Path:    path,
				Data:    data,
				DelFlag: false,
				Ver:     stat.Version,
				CVer:    stat.Cversion,
				Stat:    stat,
			}
		}

	}()

	return data, stat, watchPubCh, err
}

//获取并持续订阅数据变化, X表示采用回调处理方式
func (this *ZkSdk) GetMoreWX(path string, exitCh chan bool, hand DataHandler) ([]byte, *zk.Stat, <-chan *TPubMsg, error) {
	if !this.mRunFlag {
		return nil, nil, nil, fmt.Errorf("ZkSdk not started")
	}
	data, stat, WatchCh, err := this.GetMoreW(path, exitCh)
	if err != nil {
		return data, stat, WatchCh, err
	}
	go func() {
		var msg *TPubMsg
		var ok bool
		for {
			select {
			case msg, ok = <-WatchCh:
				if !ok {
					logger.Error("path[%s] WatchCh closed", path)
					return
				}
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}
			hand.Proc(msg.Data, msg.Stat, msg.DelFlag)
			if msg.DelFlag {
				return
			}
		}
	}()

	return data, stat, WatchCh, err
}

//获取子节点名称
func (this *ZkSdk) GetSons(path string) ([]string, *zk.Stat, error) {
	if !this.mRunFlag {
		return nil, nil, fmt.Errorf("ZkSdk not started")
	}
	data, stat, err := this.mConn.Children(path)
	return data, stat, err
}

//获取子节点，并持续订阅子节点个数的变化
func (this *ZkSdk) GetSonsMoreW(path string, exitCh chan bool) ([]string, *zk.Stat, <-chan *TPubMsg, error) {
	if !this.mRunFlag {
		return nil, nil, nil, fmt.Errorf("ZkSdk not started")
	}
	data, stat, oneEventCh, err := this.mConn.ChildrenW(path)
	if err != nil {
		return nil, stat, nil, err
	}
	watchPubCh := make(chan *TPubMsg, Default_PubChSize)
	oldCVer := stat.Cversion
	go func() {
		defer close(watchPubCh)
		var event zk.Event
		event.Type = zk.EventNotWatching
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		logger.Info("ZkSdk GetSonsMoreW[%s]", path)
		defer logger.Info("ZkSdk UnSubGetSonsMoreW[%s]", path)

		for {
			select {
			case event = <-oneEventCh:
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}

			if event.Type == zk.EventNodeDeleted {
				logger.Info("node[%s] Deleted", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}

		HEREGetSonsMoreW:
			data, stat, oneEventCh, err = this.mConn.ChildrenW(path)
			if err == zk.ErrNoNode {
				logger.Info("node[%s] noExist", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}
			if err != nil {
				logger.Error("ChildrenW[%s] err[%v] sleep[%d] and continue", path, err, Default_ConErrSleep)
				time.Sleep(time.Second * Default_ConErrSleep)
				goto HEREGetSonsMoreW
			}
			if stat.Cversion <= oldCVer {
				continue
			}
			oldCVer = stat.Cversion
			logger.Debug("node[%s] data change[%v]", path, data)
			watchPubCh <- &TPubMsg{
				Path:    path,
				Sons:    data,
				DelFlag: false,
				Ver:     stat.Version,
				CVer:    stat.Cversion,
				Stat:    stat,
			}
		}
	}()
	return data, stat, watchPubCh, err
}

//获取子节点，并持续订阅子节点个数的变化，采用回调方式处理
func (this *ZkSdk) GetSonsMoreWX(path string, exitCh chan bool, hand SonHandler) ([]string, *zk.Stat, <-chan *TPubMsg, error) {
	if !this.mRunFlag {
		return nil, nil, nil, fmt.Errorf("ZkSdk not started")
	}
	data, stat, WatchCh, err := this.GetSonsMoreW(path, exitCh)
	if err != nil {
		return data, stat, WatchCh, err
	}
	go func() {
		var msg *TPubMsg
		var ok bool
		oldSons := make(map[string]string, 0)
		_,_,oldSons = DiffNodes(data, oldSons) //这个是新加的，没有验证过，需要看看对不对
		for {
			select {
			case msg, ok = <-WatchCh:
				if !ok {
					logger.Error("path[%s] WatchCH is closed")
					return
				}
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}
			add, del, newMap := DiffNodes(msg.Sons, oldSons)
			oldSons = newMap
			hand.Proc(msg.Sons, add, del, msg.Stat, msg.DelFlag)
			if msg.DelFlag {
				return
			}
		}
	}()

	return data, stat, WatchCh, err
}

//////////////////////////////////老接口，将废弃///////////////////////////////////
//订阅数据变化
func (this *ZkSdk) WatchData(path string, exitCh chan bool, useFirst bool) (<-chan *TPubMsg, error) {
	if !this.mRunFlag {
		return nil, fmt.Errorf("ZkSdk not started")
	}
	watchPubCh := make(chan *TPubMsg, Default_PubChSize)
	go func() {
		defer close(watchPubCh)
		var event zk.Event
		event.Type = zk.EventNotWatching
		oldVer := int32(-2)
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		logger.Info("ZkSdk WatchData[%s]", path)
		defer logger.Info("ZkSdk UnWatchData[%s]", path)
		for {
			data, stat, oneEventCh, err := this.mConn.GetW(path)
			if err == zk.ErrNoNode {
				logger.Info("node[%s] noExist", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}
			if err != nil {
				logger.Error("WatchData[%s] err[%v] sleep[%d] and continue", path, err, Default_ConErrSleep)
				time.Sleep(time.Second * Default_ConErrSleep)
				continue
			}

			if !useFirst {
				oldVer = stat.Version
				useFirst = true
			}

			if stat.Version > oldVer {
				logger.Debug("node[%s] data change[%s]", path, string(data))
				watchPubCh <- &TPubMsg{
					Path:    path,
					Data:    data,
					DelFlag: false,
					Ver:     stat.Version,
					CVer:    stat.Cversion,
				}
				oldVer = stat.Version
			}

			select {
			case event = <-oneEventCh:
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}

			if event.Err != nil {
				logger.Info("Zk Watch[%s] Chan Event Type[%v] State[%v] server[%v] path[%v] err[%v]", path, event.Type, event.State, event.Server, event.Path, event.Err)
			}

			if event.Type == zk.EventNodeDeleted {
				logger.Info("node[%s] Deleted", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}
		}

	}()

	return watchPubCh, nil
}

//useFirst使用未变化的数据
func (this *ZkSdk) WatchDataX(path string, exitCh chan bool, hand DataHandler, useFirst bool) error {
	if !this.mRunFlag {
		return fmt.Errorf("ZkSdk not started")
	}
	go func() {
		var event zk.Event
		event.Type = zk.EventNotWatching
		this.mWaitGroup.Add(1)
		oldVer := int32(-2)
		defer this.mWaitGroup.Done()
		logger.Info("ZkSdk WatchDataX[%s]", path)
		defer logger.Info("ZkSdk UnWatchDataX[%s]", path)
		for {
			data, stat, oneEventCh, err := this.mConn.GetW(path)
			if err == zk.ErrNoNode {
				logger.Info("node[%s] noExist", path)
				return
			}
			if err != nil {
				logger.Error("getw[%s] err[%v]", path, err)
				time.Sleep(time.Second * Default_ConErrSleep)
				continue
			}
			if !useFirst {
				useFirst = true
				oldVer = stat.Version
			}

			if stat.Version > oldVer {
				logger.Debug("node[%s] data change[%s]", path, string(data))
				hand.Proc(data, stat, false)
				oldVer = stat.Version
			}

			select {
			case event = <-oneEventCh:
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}

			if event.Type == zk.EventNodeDeleted {
				logger.Info("node[%s] Deleted", path)
				hand.Proc(nil, stat, true)
				return
			}
		}

	}()

	return nil
}

//将被废弃
func (this *ZkSdk) WatchChild(path string, exitCh chan bool, useFirst bool) (<-chan *TPubMsg, error) {
	if !this.mRunFlag {
		return nil, fmt.Errorf("ZkSdk not started")
	}
	watchPubCh := make(chan *TPubMsg, Default_PubChSize)
	go func() {
		defer close(watchPubCh)
		var event zk.Event
		event.Type = zk.EventNotWatching
		oldCVer := int32(-2)
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		logger.Info("ZkSdk WatchChild[%s]", path)
		defer logger.Info("ZkSdk UnWatchChild[%s]", path)

		for {
			data, stat, oneEventCh, err := this.mConn.ChildrenW(path)
			if err == zk.ErrNoNode {
				logger.Info("node[%s] noExist", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}
			if err != nil {
				logger.Error("ChildrenW[%s] err[%v]", path, err)
				time.Sleep(time.Second * Default_ConErrSleep)
				continue
			}
			if !useFirst {
				useFirst = true
				oldCVer = stat.Cversion
			}
			if stat.Cversion > oldCVer {
				logger.Info("node[%s] children change[%v]", path, data)
				watchPubCh <- &TPubMsg{
					Path:    path,
					Sons:    data,
					DelFlag: false,
					Ver:     stat.Version,
					CVer:    stat.Cversion,
				}
				oldCVer = stat.Cversion
			}

			select {
			case event = <-oneEventCh:
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}

			//若该节点被删除，则退出循环，退出go程
			if event.Type == zk.EventNodeDeleted {
				logger.Info("node[%s] Deleted", path)
				watchPubCh <- &TPubMsg{
					Path:    path,
					DelFlag: true,
				}
				return
			}
		}
	}()

	return watchPubCh, nil
}

//将被废弃
func (this *ZkSdk) WatchChildX(path string, exitCh chan bool, hand SonHandler, useFirst bool) error {
	if !this.mRunFlag {
		return fmt.Errorf("ZkSdk not started")
	}
	oldSons := make(map[string]string, 0)
	go func() {
		var event zk.Event
		event.Type = zk.EventNotWatching
		oldCVer := int32(-2)
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		logger.Info("ZkSdk WatchChildX[%s]", path)
		defer logger.Info("ZkSdk UnWatchChildX[%s]", path)

		for {
			data, stat, oneEventCh, err := this.mConn.ChildrenW(path)
			if err == zk.ErrNoNode {
				logger.Info("node[%s] noExist", path)
				add, del, _ := DiffNodes(nil, oldSons)
				hand.Proc(add, del, stat, true)
				return
			}
			if err != nil {
				logger.Error("ChildrenW[%s] err[%v]", path, err)
				time.Sleep(time.Second * Default_ConErrSleep)
				continue
			}
			if !useFirst {
				useFirst = true
				oldCVer = stat.Cversion
			}

			if stat.Cversion > oldCVer {
				logger.Info("node[%s] children change[%v]", path, data)
				add, del, newMap := DiffNodes(data, oldSons)
				oldSons = newMap
				hand.Proc(data, add, del, stat, false)
				oldCVer = stat.Cversion
			}

			select {
			case event = <-oneEventCh:
			case <-this.mExitCh:
				return
			case <-exitCh:
				return
			}

			//若该节点被删除，则退出循环，退出go程
			if event.Type == zk.EventNodeDeleted {
				logger.Info("node[%s] Deleted", path)
				add, del, _ := DiffNodes(nil, oldSons)
				hand.Proc(add, del, stat, true)
				return
			}
		}
	}()

	return nil
}

/*********************************内部接口***********************************/
//zk第一次必须收到会话消息才认为ZkMgr启动成功
func (this *ZkSdk) zkFirstStateOk() {
	select {
	case this.mOneOkCh <- true:
	default:
	}
}

func (this *ZkSdk) Monitor() {
	var once sync.Once

	dealEvent := func(event *zk.Event) {
		switch (*event).State {
		case zk.StateConnected:
			logger.Info("zookeeper connected")
		case zk.StateDisconnected:
			logger.Info("zookeeper disconnected")
		case zk.StateHasSession: //新会话时必须创建临时节点
			logger.Info("zookeeper hasSession")
			once.Do(this.zkFirstStateOk)

			var err error
			this.mTmpNodesLock.Lock()
			for k, v := range this.mTmpNodes { //可能会报零时节点存在的错误
				_, err = this.mConn.Create(k, v, this.toFlag(FlagEphemeral), zk.WorldACL(zk.PermAll))
				if err != nil {
					logger.Error("Create TmpNode[%s] data[%s] err[%v]", k, v, err)
				}
			}
			this.mTmpNodesLock.Unlock()

			this.mDelNodesLock.Lock()
			for k, v := range this.mDelNodes {
				err = this.mConn.Delete(k, v)
				if err == nil {
					delete(this.mDelNodes, k)
				}
				if err == zk.ErrBadVersion {
					delete(this.mDelNodes, k)
				}
				if err == zk.ErrNoNode {
					delete(this.mDelNodes, k)
				}
				if err != nil {
					logger.Error("Must Del node[%s] ver[%d] err[%s]", k, v, err.Error())
					continue
				}
			}
			this.mDelNodesLock.Unlock()
		}
	}

	this.mWaitGroup.Add(1)
	defer this.mWaitGroup.Done()
	logger.Info("ZkSdk Monitor ok")
	for {
		select {
		case event := <-this.mEventCh:
			dealEvent(&event)
		case <-this.mExitCh:
			logger.Info("ZkSdk Monitor exit")
			return
		}
	}
}

func (this *ZkSdk) toFlag(flag int32) int32 {
	if flag == FlagNormal {
		return 0
	}
	if flag == FlagEphemeral {
		return zk.FlagEphemeral
	}
	if flag == FlagSequence {
		return zk.FlagSequence
	}
	return 0
}

func ExitRecovery() {
	time.Sleep(time.Millisecond * 100)
	if err := recover(); err != nil {
		logger.Error("runtime error:%v", err) //这里的err其实就是panic传入的内容，55
		logger.Error("stack:%v", string(debug.Stack()))
		logger.Close() //打印出日志
		os.Exit(-1)    //出现错误主动退出，重启
	}
}

func DiffNodes(newSons []string, oldSons map[string]string) ([]string, []string, map[string]string) {
	newSonsMap := make(map[string]string)
	add := make([]string, 0)
	for i := 0; i < len(newSons); i++ {
		if _, ok := oldSons[newSons[i]]; !ok {
			add = append(add, newSons[i])
		}
		newSonsMap[newSons[i]] = ""
	}
	del := make([]string, 0)
	for k, _ := range oldSons {
		if _, ok := newSonsMap[k]; !ok {
			del = append(del, k)
		}
	}
	return add, del, newSonsMap
}
