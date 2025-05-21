# Nebula2Nebula

nebulagraph 数据导入导出工具, 支持3.6.0、3.8.0等版本

## (1) Requirements
- jdk8

## (2) Get Started

```shell

# 导出schema, 生成schemas.ngql文件
java -cp nebula2nebula-1.0.jar SchemaExport --graphd <NEBULA_HOST>:9669 --user root --pass <NEBULA_PASS>

# 导入schema，导入当前路径下的schemas.ngql文件
java -cp nebula2nebula-1.0.jar SchemaImport --graphd <NEBULA_HOST>:9669 --user root --pass <NEBULA_PASS>

# nebula2csv
java -cp nebula2nebula-1.0.jar Nebula2Csv --graphd <NEBULA_HOST>:9669 --metad <NEBULA_HOST>:9559 --user root --pass <NEBULA_PASS> --csv nebula_dump --delimiter ","

# csv2nebula
java -cp nebula2nebula-1.0.jar Csv2Nebula --graphd <NEBULA_HOST>:9669 --metad <NEBULA_HOST>:9559 --user root --pass <NEBULA_PASS> --csv nebula_dump --delimiter ","

# nebula2nebula
java -cp nebula2nebula-1.0.jar Nebula2Nebula --graphd <NEBULA_HOST>:9669 --metad <NEBULA_HOST>:9559 --user root --pass <NEBULA_PASS> --t_metad <TARGET_NEBULA_HOST>:9559 --t_graphd <TARGET_NEBULA_HOST>:9669 --t_user root --t_pass <TARGET_NEBULA_PASS>

```
## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
