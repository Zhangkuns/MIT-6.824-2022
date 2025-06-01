#############################################################
## 第一步 word-count

## 构建命令
```bash
go build -race -buildmode=plugin ../mrapps/wc.go
go build -race mrsequential.go
```

## 生成标准答案
```bash
./mrsequential wc.so pg*txt
sort mr-out-0 > mr-correct-wc.txt
```

## 运行分布式系统
```bash
go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go wc.so
```

## 验证结果
```bash
sort mr-out* | grep . > mr-wc-all.txt
cmp mr-wc-all.txt mr-correct-wc.txt
diff mr-wc-all.txt mr-correct-wc.txt
```
######################################################
## 第二步 index检查
```bash
go build -race -buildmode=plugin ../mrapps/indexer.go
go build -race mrsequential.go
```

## 生成标准答案
```bash
./mrsequential indexer.so pg*txt
sort mr-out-0 > mr-correct-indexer.txt
```

## 运行分布式系统
```bash
go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go indexer.so
```

## 验证结果
```bash
sort mr-out* | grep . > mr-indexer-all.txt
cmp mr-indexer-all.txt mr-correct-indexer.txt
diff mr-indexer-all.txt mr-correct-indexer.txt
```