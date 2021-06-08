# aws-es-redis-migration

## eschekwj.py
### 安装模块
pip3 install elasticsearch

### 执行python文件
python3 estestwj.py -u1 http://source:9200 (http://10.2.16.203:9200/) -u2 https://target.es.amazonaws.com -ut 'user' -pt 'password' -i index1 index2 -m id
### 参数说明：
> `--percentage` 如果设置为100 则扫描所有Index和index下所有文档的数据校验。 默认扫描各Index下doc.count * 10% 数据
> 
> `--index_name_list` 可传Index列表， 如 -i test_index1 test_index2   如未传参，则扫描所有Index
> 
> `--mode` 默认校验文档数据（doc._source）， 如指定-m id， 则只校验doc._id 是否存在，不校验数据

- parser.add_argument('--es_url_source', '-u1', help='源ES URL，必要参数', required=True)
- parser.add_argument('--es_url_target', '-u2', help='目标ES URL，必要参数', required=True)
- parser.add_argument('--user_source', '-us', help='源ES用户名，非必要参数',default='')
- parser.add_argument('--password_source', '-ps', help='源ES密码，非必要参数',default='')
- parser.add_argument('--user_target', '-ut', help='目标ES用户名，非必要参数',default='')
- parser.add_argument('--password_target', '-pt', help='目标ES密码，非必要参数',default='')
- parser.add_argument('--index_name_list', '-i', help='Index列表，非必要参数',nargs='+')
- parser.add_argument('--check_mode', '-m', help='校验模式，data或id校验。非必要参数',default="data",type=str)
- parser.add_argument('--percentage','-p', help='随机校验百分比，非必要参数，默认10%', default=10)

#### TBD：
如果出现tuple index out of range 错误，可能是由于Python's default recursion limit is 1000. 可以注解掉py文件内sys.setrecursionlimit。

也可以从程序上进行优化。
##### _Keep optimizing_

## redis-mirror-fork.py
基于： https://github.com/alivx/redis-mirror 

### 安装模块
pip3 install redis click redis-py-cluster

### 使用
redis-cli -h {source_host} -p {source_port} monitor | python3 redis-mirror-fork.py  --sport {source_port} --shost {source_host}  --dhost {target_host} --dport {target_port} --replace

