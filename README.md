# 样本链采录台

离线实验室扫码日志的保管链核验。原始事件与来源哈希留在 SQLite，结论关联证据 ID。样本和人员均为虚构代号，不连接外部服务。

## 运行

`python -m samplechain demo` 一条命令初始化、导入、判定并输出报告路径。`python -m samplechain --help` 查看独立命令；`python -m pytest -q` 测试。运行期只用 Python 3.11 标准库。

## 数据与规则

Batch → Sample，Source → Event，Event → Issue，Sample → Decision。采集、移交、接收、入柜、取出、检测需按顺序；封签开封和复封成对，离柜时间超过 30 分钟、温度超 2–8 度、过期或作废阻断检测。扫描设备序号断号及冲突保留问题。报告包含全部输入行、来源哈希、问题和规则版本。

当前单机实现按事件时间排序；无法在缺失外部锚点时判断设备时钟准确性。人工争议处理属于后续业务，不自动推断。

安装检查：在仓库目录运行 `python -m pip install . --no-build-isolation`；本地开发也可直接运行模块命令。报告同时生成 JSON 和 Markdown，`inspect` 子命令可追踪单个对象。

审计台账：Run 由规则版本和来源摘要确定，Evidence 关联原始对象与来源，Decision 引用 Run；重复核验保持同一输入的报告稳定。

原始来源字节保存在 SQLite BLOB 中，JSON 证据包以 Base64 表示字节；修改输入文件不会覆盖已采录证据。

## 原始流镜像归档

`mirror-register` 冻结清单与冲突原件，并把分块经上传会话管线采入内容寻址归档；`mirror-upload`/`mirror-grant`/`mirror-revoke` 管理直接上传与签名凭证。分块按内容哈希寻址，上传会话可断点续传，同名不同字节不会被误判为幂等；块内校验设备 ID、序号范围、跳号/倒序/重复、签名来源与封存 epoch，字节原样留存。序号重叠且字节一致时合并证据，冲突时保留两条不可改写分支及来源，不按到达先后自动决定真值。

`mirror-publish` 冻结本次权威契约（`device_primary`/`witness_mirror`），水位只能从已证明连续且所选分支完整的前缀递增（缺序号 4 不能先发布 5–6），以设备 epoch 比较交换原子提交；读者要么拿到旧完整版本，要么拿到新完整版本。已发布的字节摘要永不改写，新证据或契约修订以新版本加替代关系发布。`mirror-stream` 读取当前完整版本，`mirror-report` 输出 JSON/Markdown 归档报告，载明规范流字节、逐行来源映射、缺口/冲突证明、封存 epoch、权威契约、版本 ID 及未选分支留存位置。镜像归档不做样本可检测性判定。
