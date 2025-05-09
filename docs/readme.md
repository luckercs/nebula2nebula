# Nebula2Nebula

nebulagraph 数据导入导出工具, 支持3.6.0、3.8.0等版本

## (1) Requirements
- jdk8
- spark-2.4.8-bin-hadoop2.7

```shell
# 需要spark2.4.8的环境
export SPARK_HOME=$(pwd)/spark-2.4.8-bin-hadoop2.7
mv spark-2.4.8-bin-hadoop2.7/conf/log4j.properties.template spark-2.4.8-bin-hadoop2.7/conf/log4j.properties
```

## (2) Get Started

```shell

# 导出所有schema，之后可以通过 nebula-console 工具或者 nebula-studio 的web页面导入schema语句执行
spark-2.4.8-bin-hadoop2.7/bin/spark-submit --master local --class NebulaSchema nebula2nebula-1.0-jar-with-dependencies.jar --graphd <NEBULA_HOST>:9669 --user root --pass <NEBULA_PASS>

# nebula2csv, 将nebulagraph数据导出到本地csv文件
spark-2.4.8-bin-hadoop2.7/bin/spark-submit --master local --class Nebula2Csv nebula2nebula-1.0-jar-with-dependencies.jar --graphd <NEBULA_HOST>:9669 --metad <NEBULA_HOST>:9559 --user root --pass <NEBULA_PASS> --csv nebula_dump --delimiter ","

# csv2nebula, 将本地csv文件导入到指定nebulagraph中
spark-2.4.8-bin-hadoop2.7/bin/spark-submit --master local --class Csv2Nebula nebula2nebula-1.0-jar-with-dependencies.jar --graphd <NEBULA_HOST>:9669 --metad <NEBULA_HOST>:9559 --user root --pass <NEBULA_PASS> --csv nebula_dump --delimiter ","

```
## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
