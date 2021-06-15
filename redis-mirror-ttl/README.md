## redis-mirror-fork.py
基于： https://github.com/alivx/redis-mirror 

### 安装模块
pip3 install redis click redis-py-cluster

### 使用
redis-cli -h {source_host} -p {source_port} monitor | python3 redis-mirror-fork.py  --sport {source_port} --shost {source_host}  --dhost {target_host} --dport {target_port} --replace

