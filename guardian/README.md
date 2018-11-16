## README

Guardian 是 Waterdrop 的子项目，用于监控 Waterdrop 的运行状态，目前仅支持以下功能：

* 监控运行于Yarn集群上的 Waterdrop 程序

* 可监控 Waterdrop 是否存活，并能够根据配置自动拉起 Waterdrop 程序

* 可监控 Waterdrop 程序运行时streaming batch是否存在堆积和延迟

* 以上两项监控如果达到阈值可发送邮件报警

* 可自定义实现不同的报警方法(Python)，如短信报警，微信报警等。


---

## 运行环境

Guardian 虽然是用python开发的，但是它已经被打包为可独立部署的程序包，不依赖任何Python环境及Python依赖包

---

## 为Guardian的代码做贡献

> 强烈建议使用Python2.7.x 作为 Guardian 的开发环境

> 建议使用virtualenv（但不是必须的）作为python运行环境切换的工具。

> 安装virtualenv方法：pip install virtualenv==1.11.6

```
# 初始化开发环境
virtualenv -p python2.7 VENV
source VENV/bin/activate

pip install -r requirements.txt

``` 

## 打包为可独立运行的服务

```
./package.sh
```

打包完成后，可以在`dist/`找到guardian_<version>.tar.gz, 解压缩后可直接运行

## 运行

```
# show help information
./dist/guardian
```
