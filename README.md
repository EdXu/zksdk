# zksdk
zookeeper客户端接口的封装，处理连续监听node，断线重连的一些问题
# 重要接口描述
//连接服务器组
func (this *ZkSdk) StartX() (ech <-chan zk.Event, err error) 
//至少连上一次才算启动成功
func (this *ZkSdk) Start(oneOk ...bool) (err error) 
//父节点不存在就创建
func (this *ZkSdk) CreateDepthNode(path string, data []byte, flag int32) (string, error)
//获取,并订阅一次变化通知
func (this *ZkSdk) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) 
//获取,并持续订阅数据变化
func (this *ZkSdk) GetMoreW(path string, exitCh chan bool) ([]byte, *zk.Stat, <-chan *utils.ZKNodeItem, error) 
//获取并持续订阅数据变化, X表示采用回调处理方式
func (this *ZkSdk) GetMoreWX(path string, exitCh chan bool, hand DataHandler) ([]byte, *zk.Stat, <-chan *utils.ZKNodeItem, error) 
//保持临时节点永活，会话再次建立时无须再创建临时节点
func (this *ZkSdk) AddTmpAlive(path string, data []byte)
//添加必须删除节点，再下次会话建立后自动删除
func (this *ZkSdk) AddDeadNode(path string, ver int32)
# 依赖
"github.com/alecthomas/log4go"
"github.com/samuel/go-zookeeper/zk"
