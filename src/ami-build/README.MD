# Building an AMI for Cluster Deployment

Inspiration for process from [Micheal Primeaux](http://michaelprimeaux.com/software-engineering/amazon/terraform/packer/apache-spark/2016/03/19/building-a-spark-cluster-in-aws-with-terraform-and-packer.html) 
and from [sixafter](https://github.com/sixafter/aws-spark-cluster-management).


##Steps:
Download spark:

```bash
wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1.tgz
```

Customize spark
```bash
tar -zxf spark-2.3.1.tgz
``` 
Modify spark as needed: see [Micheal Primeaux](http://michaelprimeaux.com/software-engineering/amazon/terraform/packer/apache-spark/2016/03/19/building-a-spark-cluster-in-aws-with-terraform-and-packer.html). 
Then ensure that scala is 2.11
```bash
./dev/change-scala-version.sh 2.11
```
Build the distro
```bash
./dev/make-distribution.sh --name custom-spark --tgz -Phadoop-2.7 -Pyarn -Dscala-2.11
```
Build AMI image
```bash
cd src/ami-build/; chmod +x build-ami.sh 
./build-ami.sh
```

